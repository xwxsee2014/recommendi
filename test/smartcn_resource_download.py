import os
import glob
import json
import random
import requests
from sqlalchemy import create_engine, Column, String, Integer, and_
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

def init_db(db_path):
    engine = create_engine(f'sqlite:///{db_path}')
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session

def process_textbooks():
    # 读取所有 info_parts_*.json 文件
    input_dir = os.path.join(os.path.dirname(__file__), '../temp_input/smartcn/textbook')
    json_files = glob.glob(os.path.join(input_dir, 'info_parts_*.json'))

    all_items = []
    for file in json_files:
        with open(file, 'r', encoding='utf-8') as f:
            items = json.load(f)
            all_items.extend(items)

    # 过滤 title 包含“统编版”的元素
    filtered = [item for item in all_items if isinstance(item, dict) and 'title' in item and '统编版' in item['title']]

    # 提取 id 和 title
    id_title_pairs = [(item['id'], item['title']) for item in filtered if 'id' in item]

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
            resp = requests.get(parts_url, timeout=10)
            if resp.status_code != 200:
                # 尝试备用域名
                parts_url = f"https://s-file-2.ykt.cbern.com.cn/zxx/ndrs/prepare_lesson/teachingmaterials/{textbook_id}/resources/parts.json"
                resp = requests.get(parts_url, timeout=10)
            resp.raise_for_status()
            part_list = resp.json()
        except Exception as e:
            print(f"Failed to fetch parts.json for {textbook_id}: {e}")
            continue

        # 课程包url列表
        part_urls = [
            f"https://s-file-1.ykt.cbern.com.cn/zxx/ndrs/prepare_lesson/teachingmaterials/{textbook_id}/resources/part_{part['id']}.json"
            for part in part_list if 'id' in part
        ]
        # 备用域名
        part_urls2 = [
            f"https://s-file-2.ykt.cbern.com.cn/zxx/ndrs/prepare_lesson/teachingmaterials/{textbook_id}/resources/part_{part['id']}.json"
            for part in part_list if 'id' in part
        ]

        for url1, url2 in zip(part_urls, part_urls2):
            try:
                resp = requests.get(url1, timeout=10)
                if resp.status_code != 200:
                    resp = requests.get(url2, timeout=10)
                resp.raise_for_status()
                course_bag = resp.json()
            except Exception as e:
                print(f"Failed to fetch course bag: {e}")
                continue

            # relations
            relations = course_bag.get('relations', {})
            resource_list = []
            if 'course_resource' in relations:
                resource_list = relations['course_resource']
            elif 'national_course_resource' in relations:
                resource_list = relations['national_course_resource']
            else:
                continue

            for res in resource_list:
                if res.get('resource_type_code_name') == '教学设计':
                    course_bag_id = res.get('id')
                    resource_type_code = res.get('resource_type_code')
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
                    else:
                        continue
                    # 请求详情
                    for detail_url in urls:
                        try:
                            resp = requests.get(detail_url, timeout=10)
                            if resp.status_code == 200:
                                detail_json = resp.json()
                                print(f"Fetched lesson plan detail for {course_bag_id} from {detail_url}")
                                break
                        except Exception as e:
                            continue
                    # 可在此处处理 detail_json
                    # ...（后续处理/保存逻辑可扩展）...
    session.close()

if __name__ == "__main__":
    process_textbooks()
    # fetch_lesson_plan_resources_for_random_subjects()  # 如需运行请取消注释
