import os
import json
from smartcn_resource_download import init_db, ResourceDownloadStatus, LessonPlanResourceMeta, TextbookTM, Textbook
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func

def supplement_resource_download_status_fields():
    """
    补全 resource_download_status 表的 resource_type_code、tag_list、tag_names 字段。
    """
    output_dir = os.path.join(os.path.dirname(__file__), '../temp_output/smartcn')
    db_path = os.path.join(output_dir, 'textbooks.db')
    Session = init_db(db_path)
    session = Session()
    all_status = session.query(ResourceDownloadStatus).all()
    for status in all_status:
        # 若 tag_list 为空，尝试补全
        if not status.tag_list:
            # 可根据 course_bag_id 查找 lesson_plan_resource_meta
            meta = session.query(LessonPlanResourceMeta).filter(LessonPlanResourceMeta.course_bag_id == status.course_bag_id).first()
            if meta:
                status.tag_list = meta.tag_list
        # 若 tag_names 为空，尝试补全
        if not status.tag_names and status.tag_list:
            try:
                tag_list = json.loads(status.tag_list)
                tag_names = [tag.get('tag_name') for tag in tag_list if isinstance(tag, dict) and 'tag_name' in tag]
                status.tag_names = ','.join(tag_names)
            except Exception:
                pass
        # 若 resource_type_code 为空，尝试补全
        if not status.resource_type_code:
            meta = session.query(LessonPlanResourceMeta).filter(LessonPlanResourceMeta.course_bag_id == status.course_bag_id).first()
            if meta:
                status.resource_type_code = meta.resource_type_code
    session.commit()
    session.close()
    print("supplement_resource_download_status_fields done.")

def supplement_lesson_plan_resource_meta():
    """
    补全 lesson_plan_resource_meta 表的 filename、filename_code 字段。
    """
    output_dir = os.path.join(os.path.dirname(__file__), '../temp_output/smartcn')
    db_path = os.path.join(output_dir, 'textbooks.db')
    Session = init_db(db_path)
    session = Session()
    all_meta = session.query(LessonPlanResourceMeta).all()
    for meta in all_meta:
        # 若 filename_code 为空，尝试补全
        if meta.filename and not meta.filename_code:
            import hashlib
            import os
            file_stem = os.path.splitext(meta.filename)[0]
            middle_file_stem = file_stem + '_middle'
            if not all(c.isalnum() or c in "_-" for c in file_stem):
                filename_code = hashlib.md5(middle_file_stem.encode("utf-8")).hexdigest()
            else:
                filename_code = middle_file_stem
            meta.filename_code = filename_code
    session.commit()
    session.close()
    print("supplement_lesson_plan_resource_meta done.")

def supplement_textbook_tm_fields():
    """
    补全 textbook_tm 表的 tag_list、filename_code 字段。
    """
    output_dir = os.path.join(os.path.dirname(__file__), '../temp_output/smartcn')
    db_path = os.path.join(output_dir, 'textbooks.db')
    Session = init_db(db_path)
    session = Session()
    all_tm = session.query(TextbookTM).all()
    for tm in all_tm:
        # 若 tag_list 为空，尝试补全
        if not tm.tag_list and tm.id:
            # 可根据 id 查找 textbook_tm_*.json
            input_dir = os.path.join(os.path.dirname(__file__), '../temp_input/smartcn/textbook_tm')
            json_files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.startswith('textbook_tm_') and f.endswith('.json')]
            found = False
            for file in json_files:
                with open(file, 'r', encoding='utf-8') as f:
                    items = json.load(f)
                    for item in items:
                        if isinstance(item, dict) and item.get('id') == tm.id:
                            tm.tag_list = json.dumps(item.get('tag_list', []), ensure_ascii=False)
                            found = True
                            break
                if found:
                    break
        # 若 filename_code 为空，尝试补全
        if tm.filename and not tm.filename_code:
            import hashlib
            import os
            file_stem = os.path.splitext(tm.filename)[0]
            middle_file_stem = file_stem + '_middle'
            if not all(c.isalnum() or c in "_-" for c in file_stem):
                filename_code = hashlib.md5(middle_file_stem.encode("utf-8")).hexdigest()
            else:
                filename_code = middle_file_stem
            tm.filename_code = filename_code
    session.commit()
    session.close()
    print("supplement_textbook_tm_fields done.")

def update_lesson_plan_downloaded_status():
    downloads_dir = os.path.join(os.path.dirname(__file__), '../temp_output/smartcn/downloads')
    output_dir = os.path.join(os.path.dirname(__file__), '../temp_output/smartcn')
    db_path = os.path.join(output_dir, 'textbooks.db')
    Session = init_db(db_path)
    session = Session()

    # 1. 遍历downloads下所有子目录，统计每个course_bag_id下pdf数量
    course_bag_pdf_count = {}
    if os.path.exists(downloads_dir):
        for course_bag_id in os.listdir(downloads_dir):
            subdir = os.path.join(downloads_dir, course_bag_id)
            if os.path.isdir(subdir):
                pdf_count = 0
                for root, _, files in os.walk(subdir):
                    for file in files:
                        if file.lower().endswith('.pdf'):
                            pdf_count += 1
                course_bag_pdf_count[course_bag_id] = pdf_count

    # 2. 更新ResourceDownloadStatus表
    all_status = session.query(ResourceDownloadStatus).all()
    updated_ids = set()
    for status in all_status:
        cbid = status.course_bag_id
        if cbid in course_bag_pdf_count:
            status.lesson_plan = course_bag_pdf_count[cbid]
            updated_ids.add(cbid)
        else:
            status.lesson_plan = 0
    session.commit()

    # 3. 输出每个course_bag_id的pdf数量
    for cbid, count in course_bag_pdf_count.items():
        print(f"{cbid}: {count} PDFs")

    # 4. 统计ResourceDownloadStatus中lesson_plan的总和
    total = session.query(func.sum(ResourceDownloadStatus.lesson_plan)).scalar() or 0
    print(f"Total lesson plan PDFs recorded in DB: {total}")

    # 5. 按textbook_id分组统计lesson_plan数量，并更新Textbook表
    textbook_lesson_plan = session.query(
        ResourceDownloadStatus.textbook_id,
        func.sum(ResourceDownloadStatus.lesson_plan)
    ).group_by(ResourceDownloadStatus.textbook_id).all()
    for textbook_id, lesson_plan_num in textbook_lesson_plan:
        if textbook_id:
            tb = session.query(Textbook).filter(Textbook.id == textbook_id).first()
            if tb:
                tb.downloaded_lesson_plan_num = lesson_plan_num or 0
    session.commit()

    session.close()
    return total

if __name__ == "__main__":
    supplement_resource_download_status_fields()
    supplement_lesson_plan_resource_meta()
    supplement_textbook_tm_fields()
    update_lesson_plan_downloaded_status()
