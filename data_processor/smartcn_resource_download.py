import os
import glob
import json
import random
import requests
import time
import hashlib
from sqlalchemy import create_engine, Column, String, Integer, and_, Boolean, func, text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import JSON as SQLAlchemyJSON  # 兼容性考虑，后面用String存json

# 1. 定义数据库模型
Base = declarative_base()

class Textbook(Base):
    __tablename__ = 'textbooks'
    id = Column(String, primary_key=True)
    title = Column(String)
    downloaded_lesson_plan_num = Column(Integer, default=0)
    downloaded_video_num = Column(Integer, default=0)
    downloaded_slides_num = Column(Integer, default=0)
    downloaded_learning_task_num = Column(Integer, default=0)
    downloaded_worksheet_num = Column(Integer, default=0)

class ResourceDownloadStatus(Base):
    __tablename__ = 'resource_download_status'
    course_bag_id = Column(String, primary_key=True)
    textbook_id = Column(String)
    lesson_plan = Column(Integer, default=0)  # 0=未下载，>0=已下载数量
    video = Column(Integer, default=0)
    slides = Column(Integer, default=0)
    learning_task = Column(Integer, default=0)
    worksheet = Column(Integer, default=0)
    resource_type_code = Column(String)
    tag_list = Column(String)  # 新增，存储为JSON字符串
    tag_names = Column(String)  # 新增，逗号分隔

class NoLessonPlanResource(Base):
    __tablename__ = 'no_lesson_plan_resource'
    course_bag_id = Column(String, primary_key=True)

class NoLessonPlanTextbook(Base):
    __tablename__ = 'no_lesson_plan_textbook'
    textbook_id = Column(String, primary_key=True)

class TextbookTM(Base):
    __tablename__ = 'textbook_tm'
    id = Column(String, primary_key=True)
    tag_names = Column(String)
    tag_list = Column(String)  # 新增，存储为JSON字符串
    downloaded = Column(Integer, default=0)
    processed = Column(Integer, default=0)
    # 新增字段
    resource_type_code = Column(String)
    resource_type_code_name = Column(String)
    container_id = Column(String)
    filename = Column(String)
    filename_code = Column(String)  # 新增字段

    @staticmethod
    def get_all_prcessed_ids():
        """
        获取所有processed=1的id
        """
        output_dir = os.path.join(os.path.dirname(__file__), '../temp_output/smartcn')
        db_path = os.path.join(output_dir, 'textbooks.db')
        Session = init_db(db_path)
        session = Session()
        ids = [row.id for row in session.query(TextbookTM.id).filter(TextbookTM.processed == 1).all()]
        session.close()
        return ids

class LessonPlanResourceMeta(Base):
    __tablename__ = 'lesson_plan_resource_meta'
    id = Column(String, primary_key=True)
    resource_type_code = Column(String)
    resource_type_code_name = Column(String)
    container_id = Column(String)
    tag_list = Column(String)  # 存储为JSON字符串
    course_bag_id = Column(String)
    filename = Column(String)  # 新增字段
    filename_code = Column(String)  # 新增字段


def init_db(db_path):
    engine = create_engine(f'sqlite:///{db_path}')
    Base.metadata.create_all(engine)
    # column update migration for sqlite
    # with engine.connect() as conn:
    #     res = conn.execute(text("PRAGMA table_info(resource_download_status)")).fetchall()
    #     columns = [r[1] for r in res]
    #     if 'resource_type_code' not in columns:
    #         conn.execute(text("ALTER TABLE resource_download_status ADD COLUMN resource_type_code TEXT"))
    Session = sessionmaker(bind=engine)
    return Session

# 全局 x-nd-auth header
X_ND_AUTH = 'MAC id="7F938B205F876FC3A30551F3A49313836755C0D451C08FC17466435A155BBD9BCFBFF3776F22B0460E3F711306236F1764324EB8AA1C4A9D",nonce="1750058593311:N9QJV128",mac="XzaP38z2kkzMSeCToWSyuNho3pcN89KMtNBpsvju2iM="'

# 全局资源数量上限
data_num_threshold = {
    "lesson_plan": 500
}

def detail_download_pre_check(course_bag_id, session, check_types):
    """
    检查指定course_bag_id的各资源类型是否都应跳过下载。
    返回: skip_download(bool), skip_map(dict)，如 skip_map={"lesson_plan": True/False, ...}
    """
    skip_map = {}
    for rtype in check_types:
        if rtype == 'lesson_plan':
            no_lp = session.query(NoLessonPlanResource).filter(NoLessonPlanResource.course_bag_id == course_bag_id).first()
            status = session.query(ResourceDownloadStatus).filter(ResourceDownloadStatus.course_bag_id == course_bag_id).first()
            if no_lp or (status and status.lesson_plan and status.lesson_plan > 0):
                skip_map['lesson_plan'] = True
            else:
                skip_map['lesson_plan'] = False
        else:
            skip_map[rtype] = True
    skip_download = all(skip_map.values())
    return skip_download, skip_map

def extract_resource_list_from_detail_json(detail_json):
    """
    从 detail_json 的 relations 字段提取所有资源，平铺为 resource_list。
    """
    relations = detail_json.get('relations', {})
    resource_list = []
    for v in relations.values():
        if isinstance(v, list):
            resource_list.extend(v)
    return resource_list

def download_lesson_plan_pdf(res, course_bag_id, pdf_index, headers, session):
    """
    下载教学设计 PDF，插入 LessonPlanResourceMeta，返回 (是否下载, 文件名, 文件名编码)
    """
    import hashlib
    import time
    import os
    pdf_downloaded = False
    filename = None
    filename_code = None
    tag_list = res.get('tag_list', [])
    ti_items = res.get('ti_items', [])
    for ti_item in ti_items:
        if ti_item.get('ti_file_flag') == 'pdf' or ti_item.get('ti_format') == 'pdf':
            ti_storages = ti_item.get('ti_storages', [])
            for storage in ti_storages:
                if isinstance(storage, str):
                    pdf_url = storage
                elif isinstance(storage, dict):
                    pdf_url = storage.get('url')
                else:
                    continue
                if not pdf_url:
                    continue
                out_dir = os.path.join(os.path.dirname(__file__), f'../temp_output/smartcn/downloads/{course_bag_id}/lesson_plan')
                os.makedirs(out_dir, exist_ok=True)
                orig_filename = os.path.basename(pdf_url.split('?')[0])
                filename = f"{pdf_index:03d}_{orig_filename}"
                file_stem = os.path.splitext(filename)[0]
                middle_file_stem = file_stem + '_middle'
                if not all(c.isalnum() or c in "_-" for c in file_stem):
                    filename_code = hashlib.md5(middle_file_stem.encode("utf-8")).hexdigest()
                else:
                    filename_code = middle_file_stem
                out_path = os.path.join(out_dir, filename)
                try:
                    time.sleep(1)
                    resp = requests.get(pdf_url, timeout=15, headers=headers)
                    if resp.status_code == 401 or resp.status_code == 403:
                        raise RuntimeError("x-nd-auth header expired or invalid, please update it and retry.")
                    if resp.status_code == 200:
                        with open(out_path, 'wb') as f:
                            f.write(resp.content)
                        print(f"Downloaded PDF: {out_path}")
                        pdf_downloaded = True
                        # 插入 LessonPlanResourceMeta
                        meta_id = res.get('id')
                        container_id = res.get('container_id')
                        lesson_plan_resource_type_code = res.get('resource_type_code', '')
                        resource_type_code_name = res.get('resource_type_code_name', '')
                        tag_list_json_res = json.dumps(tag_list, ensure_ascii=False)
                        session.merge(LessonPlanResourceMeta(
                            id=meta_id,
                            resource_type_code=lesson_plan_resource_type_code,
                            resource_type_code_name=resource_type_code_name,
                            container_id=container_id,
                            tag_list=tag_list_json_res,
                            course_bag_id=course_bag_id,
                            filename=filename,
                            filename_code=filename_code
                        ))
                        session.commit()
                        return True, filename, filename_code
                except Exception as e:
                    print(f"Failed to download PDF from {pdf_url}: {e}")
            # 只下载一个 pdf
            if pdf_downloaded:
                break
    return False, filename, filename_code

def update_lesson_plan_db(session, textbook_id, course_bag_id, lesson_plan_downloaded_count, detail_json, tag_list_json, tag_names_str):
    """
    更新 Textbook, ResourceDownloadStatus 表
    """
    if lesson_plan_downloaded_count > 0:
        tb = session.query(Textbook).filter(Textbook.id == textbook_id).first()
        if tb:
            tb.downloaded_lesson_plan_num = (tb.downloaded_lesson_plan_num or 0) + lesson_plan_downloaded_count
        status = session.query(ResourceDownloadStatus).filter(ResourceDownloadStatus.course_bag_id == course_bag_id).first()
        if not status:
            status = ResourceDownloadStatus(
                course_bag_id=course_bag_id,
                textbook_id=textbook_id,
                lesson_plan=lesson_plan_downloaded_count,
                resource_type_code=detail_json.get('resource_type_code'),
                tag_list=tag_list_json,
                tag_names=tag_names_str,
            )
            session.add(status)
        else:
            status.lesson_plan = (status.lesson_plan or 0) + lesson_plan_downloaded_count
            status.tag_list = tag_list_json
            status.tag_names = tag_names_str
        session.commit()
        total = session.query(Textbook).with_entities(
            func.sum(Textbook.downloaded_lesson_plan_num)
        ).scalar() or 0
        print(f"当前已下载lesson_plan数量: {total}/{data_num_threshold['lesson_plan']}")
        if total >= data_num_threshold["lesson_plan"]:
            print(f"已达到lesson_plan资源数量上限: {data_num_threshold['lesson_plan']}，程序终止。")
            session.close()
            exit(0)

def process_lesson_plan_detail(detail_json, course_bag_id, textbook_id, session, skip_map=None):
    if skip_map is not None and skip_map.get("lesson_plan", False):
        print(f"The lesson plan for {course_bag_id} has already been processed, skipping...")
        return
    print(f"Fetched lesson plan detail for {course_bag_id}")
    resource_list = extract_resource_list_from_detail_json(detail_json)
    tag_list_json = json.dumps(detail_json.get('tag_list', []), ensure_ascii=False)
    tag_names_str = ','.join([tag.get('tag_name') for tag in detail_json.get('tag_list', []) if isinstance(tag, dict) and 'tag_name' in tag])
    headers = {
        "x-nd-auth": X_ND_AUTH
    }
    resource_tags = []
    lesson_plan_downloaded_count = 0
    pdf_index = 1
    has_jiaoxuesheji_tag = False
    for res in resource_list:
        tag_list = res.get('tag_list', [])
        for tag in tag_list:
            tag_name = tag.get('tag_name')
            if tag_name:
                resource_tags.append(tag_name)
        has_jiaoxuesheji = any(tag.get('tag_name') == '教学设计' for tag in tag_list)
        if has_jiaoxuesheji:
            has_jiaoxuesheji_tag = True
            downloaded, _, _ = download_lesson_plan_pdf(res, course_bag_id, pdf_index, headers, session)
            if downloaded:
                lesson_plan_downloaded_count += 1
                pdf_index += 1
    print(f"Resource tag names found: {', '.join(set(resource_tags))}")
    if not has_jiaoxuesheji_tag:
        if not session.query(NoLessonPlanResource).filter_by(course_bag_id=course_bag_id).first():
            session.add(NoLessonPlanResource(course_bag_id=course_bag_id))
            session.commit()
        print(f"该资源没有lesson plan，skipping: {course_bag_id}")
        return
    update_lesson_plan_db(session, textbook_id, course_bag_id, lesson_plan_downloaded_count, detail_json, tag_list_json, tag_names_str)

def fetch_lesson_plan_resources_for_random_subjects():
    output_dir = os.path.join(os.path.dirname(__file__), '../temp_output/smartcn')
    db_path = os.path.join(output_dir, 'textbooks.db')
    Session = init_db(db_path)
    session = Session()

    # 查询不在no_lesson_plan_textbook中的 textbook
    no_lp_tb_ids = set([row.textbook_id for row in session.query(NoLessonPlanTextbook).all()])
    textbooks = session.query(Textbook).filter(Textbook.downloaded_lesson_plan_num < 50).all()
    # 直接过滤掉 no_lesson_plan_textbook 中存在的记录
    filtered_textbooks = [tb for tb in textbooks if tb.id not in no_lp_tb_ids]
    # 取前10个
    selected = filtered_textbooks[:10]

    for tb in selected:
        textbook_id = tb.id
        print(f"Processing textbook: {tb.title} ({textbook_id})")
        # 获取 parts.json
        parts_url = f"https://s-file-1.ykt.cbern.com.cn/zxx/ndrs/prepare_lesson/teachingmaterials/{textbook_id}/resources/parts.json"
        try:
            time.sleep(1)
            resp = requests.get(parts_url, timeout=10)
            if resp.status_code != 200:
                # 尝试备用域名
                parts_url = f"https://s-file-2.ykt.cbern.com.cn/zxx/ndrs/prepare_lesson/teachingmaterials/{textbook_id}/resources/parts.json"
                time.sleep(1)
                resp = requests.get(parts_url, timeout=10)
            resp.raise_for_status()
            part_list = resp.json()
        except Exception as e:
            print(f"Failed to fetch parts.json for {textbook_id}: {e}")
            continue

        part_urls = [part for part in part_list]

        all_course_bags_skipped = True  # 标记该 textbook 下所有 course_bag 是否 skip

        for url1 in part_urls:
            print(f"Fetching course bag from {url1}")
            try:
                time.sleep(1)
                resp = requests.get(url1, timeout=10)
                resp.raise_for_status()
                course_bags = resp.json()
            except Exception as e:
                print(f"Failed to fetch course bag: {e}")
                continue

            for course_bag in course_bags:
                course_bag_id = course_bag.get('id')
                skip_download, skip_map = detail_download_pre_check(course_bag_id, session, ['lesson_plan'])
                if not skip_download:
                    all_course_bags_skipped = False
                if skip_download:
                    print(f"Skipping course bag {course_bag_id} due to all resources already processed...")
                    continue
                resource_type_code = course_bag.get('resource_type_code')
                detail_json = None
                if resource_type_code == 'elite_lesson':
                    urls = [
                        f"https://s-file-1.ykt.cbern.com.cn/zxx/ndrv2/resources/{course_bag_id}.json",
                        f"https://s-file-2.ykt.cbern.com.cn/zxx/ndrv2/resources/{course_bag_id}.json"
                    ]
                elif resource_type_code == 'national_lesson':
                    urls = [
                        f"https://s-file-1.ykt.cbern.com.cn/zxx/ndrv2/national_lesson/resources/details/{course_bag_id}.json",
                        f"https://s-file-2.ykt.cbern.com.cn/zxx/ndrv2/national_lesson/resources/details/{course_bag_id}.json"
                    ]
                elif resource_type_code == 'prepare_lesson':
                    urls = [
                        f"https://s-file-1.ykt.cbern.com.cn/zxx/ndrv2/prepare_lesson/resources/details/{course_bag_id}.json",
                        f"https://s-file-2.ykt.cbern.com.cn/zxx/ndrv2/prepare_lesson/resources/details/{course_bag_id}.json"
                    ]
                else:
                    print(f"Unsupported resource_type_code: {resource_type_code} for course_bag_id: {course_bag_id}")
                    continue
                for detail_url in urls:
                    try:
                        time.sleep(1)
                        resp = requests.get(detail_url, timeout=10)
                        if resp.status_code == 200:
                            detail_json = resp.json()
                            relations = detail_json.get('relations', {})
                            resource_list = []
                            for v in relations.values():
                                if isinstance(v, list):
                                    resource_list.extend(v)
                            process_lesson_plan_detail(detail_json, course_bag_id, textbook_id, session, skip_map)
                            break
                    except Exception as e:
                        import traceback
                        traceback.print_exc()
                        print(f"Failed to fetch detail for {course_bag_id} from {detail_url}: {e}")
                        continue

        # 如果所有 course_bag 都 skip，则记录 textbook_id 到 no_lesson_plan_textbook
        if all_course_bags_skipped:
            if not session.query(NoLessonPlanTextbook).filter_by(textbook_id=textbook_id).first():
                session.add(NoLessonPlanTextbook(textbook_id=textbook_id))
                session.commit()
            print(f"All course_bags for textbook {textbook_id} skipped, recorded in no_lesson_plan_textbook.")

    session.close()


def fetch_textbook_tm_resources():
    """
    遍历 textbook_tm 表中 downloaded=0 且 tag_names 包含'统编版'的行，下载PDF资源。
    同时补充 resource_type_code、resource_type_code_name、container_id、filename、tag_list 字段。
    """
    output_dir = os.path.join(os.path.dirname(__file__), '../temp_output/smartcn')
    db_path = os.path.join(output_dir, 'textbooks.db')
    Session = init_db(db_path)
    session = Session()

    # 查询 textbook_tm 表，downloaded=0 且 tag_names 包含'人教版'
    tm_rows = session.query(TextbookTM).filter(
        (TextbookTM.downloaded == False) | (TextbookTM.downloaded == None),
        TextbookTM.tag_names.like('%人教版%')
    ).all()

    downloaded_count = session.query(TextbookTM).filter(TextbookTM.downloaded == True).count()

    headers = {
        "x-nd-auth": X_ND_AUTH
    }

    for tm in tm_rows:
        textbook_tm_id = tm.id
        print(f"Processing textbook_tm: {textbook_tm_id} (tags: {tm.tag_names})")
        detail_url = f"https://s-file-2.ykt.cbern.com.cn/zxx/ndrv2/resources/tch_material/details/{textbook_tm_id}.json"
        detail_json = None
        try:
            time.sleep(1)
            resp = requests.get(detail_url, timeout=15)
            resp.raise_for_status()
            detail_json = resp.json()
        except Exception as e:
            print(f"Failed to fetch detail for {textbook_tm_id}: {e}")
            tm.processed = True
            session.commit()
            continue

        # 获取并保存 resource_type_code, resource_type_code_name, container_id, filename, tag_list
        resource_type_code = detail_json.get('resource_type_code')
        resource_type_code_name = detail_json.get('resource_type_code_name')
        container_id = detail_json.get('container_id')
        tag_list = detail_json.get('tag_list', [])
        tag_list_json = json.dumps(tag_list, ensure_ascii=False)
        filename = None
        filename_code = None

        ti_items = detail_json.get('ti_items', [])
        pdf_downloaded = False
        pdf_index = 1
        for ti_item in ti_items:
            if ti_item.get('ti_format') == 'pdf':
                ti_storages = ti_item.get('ti_storages', [])
                for storage in ti_storages:
                    print(f"Processing storage: {storage}")
                    pdf_url = storage if isinstance(storage, str) else storage.get('url') if isinstance(storage, dict) else None
                    if not pdf_url:
                        continue
                    out_dir = os.path.join(os.path.dirname(__file__), f'../temp_output/smartcn/tm_downloads/{textbook_tm_id}')
                    os.makedirs(out_dir, exist_ok=True)
                    orig_filename = os.path.basename(pdf_url.split('?')[0])
                    filename = f"{pdf_index:03d}_{orig_filename}"
                    out_path = os.path.join(out_dir, filename)
                    # 生成 filename_code
                    file_stem = os.path.splitext(filename)[0]
                    middle_file_stem = file_stem + '_middle'
                    if not all(c.isalnum() or c in "_-" for c in file_stem):
                        filename_code = hashlib.md5(middle_file_stem.encode("utf-8")).hexdigest()
                    else:
                        filename_code = middle_file_stem
                    try:
                        time.sleep(3)
                        resp = requests.get(pdf_url, timeout=15, headers=headers)
                        if resp.status_code == 200:
                            with open(out_path, 'wb') as f:
                                f.write(resp.content)
                            print(f"Downloaded PDF: {out_path}")
                            pdf_downloaded = True
                            pdf_index += 1
                            break  # 当前 ti_item 只下载一个 pdf
                    except Exception as e:
                        print(f"Failed to download PDF from {pdf_url}: {e}")
                # 若已下载，跳出 ti_item 循环
                if pdf_downloaded:
                    break

        # 更新字段
        tm.resource_type_code = resource_type_code
        tm.resource_type_code_name = resource_type_code_name
        tm.container_id = container_id
        tm.filename = filename
        tm.filename_code = filename_code  # 新增
        tm.tag_list = tag_list_json

        tm.processed = True
        if pdf_downloaded:
            tm.downloaded = True
            downloaded_count += 1
        session.commit()
        print(f"Current downloaded count: {downloaded_count}")

    # 统计并显示 downloaded=1 的数量
    total_downloaded = session.query(TextbookTM).filter(TextbookTM.downloaded == True).count()
    print(f"Total textbook_tm downloaded: {total_downloaded}")

    session.close()

if __name__ == "__main__":
    fetch_lesson_plan_resources_for_random_subjects()
    # fetch_textbook_tm_resources()
    pass
