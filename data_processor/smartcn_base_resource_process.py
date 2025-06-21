import os
import glob
import json
from smartcn_resource_download import Textbook, TextbookTM, init_db

def process_textbook_tms():
    # 读取所有 textbook_tm_*.json 文件
    input_dir = os.path.join(os.path.dirname(__file__), '../temp_input/smartcn/textbook_tm')
    json_files = glob.glob(os.path.join(input_dir, 'textbook_tm_*.json'))

    all_items = []
    for file in json_files:
        with open(file, 'r', encoding='utf-8') as f:
            items = json.load(f)
            all_items.extend(items)

    id_tag_names_pairs = []
    for item in all_items:
        if isinstance(item, dict) and 'id' in item and 'tag_list' in item:
            id_ = item['id']
            tag_list = item.get('tag_list', [])
            tag_names = [tag.get('tag_name') for tag in tag_list if isinstance(tag, dict) and 'tag_name' in tag]
            tag_names_str = ','.join(tag_names)
            id_tag_names_pairs.append((id_, tag_names_str))

    # 保存到sqlite
    output_dir = os.path.join(os.path.dirname(__file__), '../temp_output/smartcn')
    os.makedirs(output_dir, exist_ok=True)
    db_path = os.path.join(output_dir, 'textbooks.db')
    Session = init_db(db_path)
    session = Session()

    for id_, tag_names in id_tag_names_pairs:
        session.merge(TextbookTM(id=id_, tag_names=tag_names))
    session.commit()
    session.close()

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

if __name__ == "__main__":
    process_textbooks()
    process_textbook_tms()
