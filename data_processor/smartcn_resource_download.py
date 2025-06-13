import os
import glob
import json
import random
import requests
import time
from sqlalchemy import create_engine, Column, String, Integer, and_, Boolean, func
from sqlalchemy.orm import declarative_base, sessionmaker

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
    lesson_plan = Column(Integer, default=0)  # 1=已下载, 0=未下载
    video = Column(Integer, default=0)
    slides = Column(Integer, default=0)
    learning_task = Column(Integer, default=0)
    worksheet = Column(Integer, default=0)

def init_db(db_path):
    engine = create_engine(f'sqlite:///{db_path}')
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session

# 全局 x-nd-auth header
X_ND_AUTH = 'MAC id="7F938B205F876FC3A30551F3A49313836755C0D451C08FC17466435A155BBD9BCFBFF3776F22B0460E3F711306236F1764324EB8AA1C4A9D",nonce="1749809443803:TJ647GFB",mac="/7UN506SW+juOhbU95YxF2zQAH68U2sewTi35j22lMY="'

# 全局资源数量上限
data_num_threshold = {
    "lesson_plan": 100
}

def process_textbooks():
    # 读取所有 info_parts_*.json 文件
    input_dir = os.path.join(os.path.dirname(__file__), '../temp_input/smartcn/textbook')
    json_files = glob.glob(os.path.join(input_dir, 'info_parts_*.json'))

    all_items = []
    for file in json_files:
        with open(file, 'r', encoding='utf-8') as f:
            items = json.load(f)
            all_items.extend(items)

    # 不再过滤 title 包含“统编版”的元素，直接提取所有有id和title的
    id_title_pairs = [(item['id'], item['title']) for item in all_items if isinstance(item, dict) and 'id' in item and 'title' in item]

    # 保存到sqlite
    output_dir = os.path.join(os.path.dirname(__file__), '../temp_output/smartcn')
    os.makedirs(output_dir, exist_ok=True)
    db_path = os.path.join(output_dir, 'textbooks.db')
    Session = init_db(db_path)
    session = Session()

    # 6. 插入数据
    for id_, title in id_title_pairs:
        session.merge(Textbook(id=id_, title=title))
    session.commit()
    session.close()

def process_lesson_plan_detail(detail_json, course_bag_id, textbook_id, session):
    print(f"Fetched lesson plan detail for {course_bag_id}")
    # 检查是否已下载过
    status = session.query(ResourceDownloadStatus).filter(ResourceDownloadStatus.course_bag_id == course_bag_id).first()
    if status and status.lesson_plan:
        return
    # relations下所有list类型value平铺为resource_list
    relations = detail_json.get('relations', {})
    resource_list = []
    for v in relations.values():
        if isinstance(v, list):
            resource_list.extend(v)
    # 遍历 resource_list，找所有tag_list包含"教学设计"的资源
    headers = {
        "x-nd-auth": X_ND_AUTH
    }
    resource_tags = []
    lesson_plan_downloaded_count = 0
    pdf_index = 1  # 用于命名pdf文件的序号
    for res in resource_list:
        tag_list = res.get('tag_list', [])
        # 收集所有tag_name
        for tag in tag_list:
            tag_name = tag.get('tag_name')
            if tag_name:
                resource_tags.append(tag_name)
        # 判断是否有tag_name为"教学设计"
        has_jiaoxuesheji = any(tag.get('tag_name') == '教学设计' for tag in tag_list)
        if has_jiaoxuesheji:
            ti_items = res.get('ti_items', [])
            for ti_item in ti_items:
                if ti_item.get('ti_file_flag') == 'pdf':
                    ti_storages = ti_item.get('ti_storages', [])
                    for storage in ti_storages:
                        if isinstance(storage, str):
                            # 处理字符串类型的storage
                            pdf_url = storage
                        elif isinstance(storage, dict):
                            # 处理字典类型的storage
                            if 'url' in storage:
                                pdf_url = storage['url']
                            else:
                                continue
                        else:
                            continue
                        if not pdf_url:
                            continue
                        out_dir = os.path.join(os.path.dirname(__file__), f'../temp_output/smartcn/downloads/{course_bag_id}/lesson_plan')
                        os.makedirs(out_dir, exist_ok=True)
                        orig_filename = os.path.basename(pdf_url.split('?')[0])
                        filename = f"{pdf_index:03d}_{orig_filename}"
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
                                lesson_plan_downloaded_count += 1
                                pdf_index += 1
                                break  # 当前ti_item只下载一个pdf
                        except Exception as e:
                            print(f"Failed to download PDF from {pdf_url}: {e}")
                    # 不break，继续下一个ti_item
    print(f"Resource tag names found: {', '.join(set(resource_tags))}")
    # 更新数据库
    if lesson_plan_downloaded_count > 0:
        tb = session.query(Textbook).filter(Textbook.id == textbook_id).first()
        if tb:
            tb.downloaded_lesson_plan_num = (tb.downloaded_lesson_plan_num or 0) + lesson_plan_downloaded_count
        status = session.query(ResourceDownloadStatus).filter(ResourceDownloadStatus.course_bag_id == course_bag_id).first()
        if not status:
            status = ResourceDownloadStatus(course_bag_id=course_bag_id, textbook_id=textbook_id, lesson_plan=lesson_plan_downloaded_count)
            session.add(status)
        else:
            status.lesson_plan = (status.lesson_plan or 0) + lesson_plan_downloaded_count
        session.commit()
        total = session.query(Textbook).with_entities(
            func.sum(Textbook.downloaded_lesson_plan_num)
        ).scalar() or 0
        print(f"当前已下载lesson_plan数量: {total}/{data_num_threshold['lesson_plan']}")
        if total >= data_num_threshold["lesson_plan"]:
            print(f"已达到lesson_plan资源数量上限: {data_num_threshold['lesson_plan']}，程序终止。")
            session.close()
            exit(0)

def fetch_lesson_plan_resources_for_random_subjects():
    # 支持的学科关键词
    subjects = ['语文', '数学', '英语', '物理', '化学', '生物', '历史', '地理', '政治', '科学', '信息', '音乐', '美术', '体育']
    output_dir = os.path.join(os.path.dirname(__file__), '../temp_output/smartcn')
    db_path = os.path.join(output_dir, 'textbooks.db')
    Session = init_db(db_path)
    session = Session()

    # 查询符合条件的 textbook
    textbooks = session.query(Textbook).filter(Textbook.downloaded_lesson_plan_num < 10).all()
    # 按学科分组
    subject_textbook_map = {}
    for subj in subjects:
        for tb in textbooks:
            if subj in tb.title and subj not in subject_textbook_map:
                subject_textbook_map[subj] = tb
                break
    # 随机选10个不同学科
    selected = list(subject_textbook_map.values())
    if len(selected) > 10:
        selected = random.sample(selected, 10)

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

        # 课程包url列表
        part_urls = [part for part in part_list]

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
                resource_type_code = course_bag.get('resource_type_code')
                print(f"Processed course bag: resource_type_code - {resource_type_code}")
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
                # 请求详情
                for detail_url in urls:
                    try:
                        time.sleep(1)
                        resp = requests.get(detail_url, timeout=10)
                        if resp.status_code == 200:
                            detail_json = resp.json()
                            process_lesson_plan_detail(detail_json, course_bag_id, textbook_id, session)
                            break
                    except Exception as e:
                        import traceback
                        traceback.print_exc()
                        print(f"Failed to fetch detail for {course_bag_id} from {detail_url}: {e}")
                        continue
    session.close()

if __name__ == "__main__":
    # process_textbooks()
    fetch_lesson_plan_resources_for_random_subjects()
    # process_textbooks()
    fetch_lesson_plan_resources_for_random_subjects()
    fetch_lesson_plan_resources_for_random_subjects()
