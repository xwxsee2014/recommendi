import os
import json

from magic_pdf.data.data_reader_writer import FileBasedDataWriter, FileBasedDataReader
from magic_pdf.data.dataset import PymuDocDataset
from magic_pdf.model.doc_analyze_by_custom_model import doc_analyze
from magic_pdf.config.enums import SupportedPdfParseMethod
from sqlalchemy import create_engine, Column, String, Integer, func
from sqlalchemy.orm import declarative_base, sessionmaker, aliased
from smartcn_resource_download import ResourceDownloadStatus

CLASSIFIED_JSON = os.path.join("temp_output", "k12", "classified_result.json")
INPUT_BASE = os.path.join("temp_input", "k12", "download_files")
OUTPUT_BASE = os.path.join("temp_output", "k12", "pdf_process")

def process_pdf(pdf_path, output_dir, pdf_tag):
    os.makedirs(output_dir, exist_ok=True)
    image_dir = os.path.join(output_dir, "images")
    os.makedirs(image_dir, exist_ok=True)
    image_writer = FileBasedDataWriter(image_dir)
    md_writer = FileBasedDataWriter(output_dir)

    reader = FileBasedDataReader("")
    pdf_bytes = reader.read(pdf_path)
    name_without_suff = pdf_tag

    ds = PymuDocDataset(pdf_bytes)
    if ds.classify() == SupportedPdfParseMethod.OCR:
        infer_result = ds.apply(doc_analyze, ocr=True)
        pipe_result = infer_result.pipe_ocr_mode(image_writer)
    else:
        infer_result = ds.apply(doc_analyze, ocr=False)
        pipe_result = infer_result.pipe_txt_mode(image_writer)
    infer_result.draw_model(os.path.join(output_dir, f"{name_without_suff}_model.pdf"))
    pipe_result.draw_layout(os.path.join(output_dir, f"{name_without_suff}_layout.pdf"))
    pipe_result.draw_span(os.path.join(output_dir, f"{name_without_suff}_spans.pdf"))
    pipe_result.dump_md(md_writer, f"{name_without_suff}.md", os.path.basename(image_dir))
    pipe_result.dump_content_list(md_writer, f"{name_without_suff}_content_list.json", os.path.basename(image_dir))
    pipe_result.dump_middle_json(md_writer, f'{name_without_suff}_middle.json')

def process_pdf_from_path(pdf_path):
    """
    支持直接输入pdf路径，然后解析
    """
    if not os.path.exists(pdf_path):
        print(f"PDF文件不存在: {pdf_path}")
        return
    # 自动推断输出目录和pdf_tag
    base_name = os.path.splitext(os.path.basename(pdf_path))[0]
    output_dir = os.path.join("temp_output", "k12", "single_pdf_process", base_name)
    process_pdf(pdf_path, output_dir, "quiz")

def traverse_and_process():
    with open(CLASSIFIED_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    def walk(d):
        if isinstance(d, dict):
            for v in d.values():
                walk(v)
        elif isinstance(d, list):
            for item in d:
                folder = item.get("folder")
                if not folder:
                    continue
                folder_path = os.path.join(INPUT_BASE, folder)
                output_dir = os.path.join(OUTPUT_BASE, folder)
                if not os.path.exists(folder_path):
                    # create the folder if it doesn't exist
                    os.makedirs(folder_path, exist_ok=True)
                if item.get("has_lesson_plan_pdf"):
                    pdf_path = os.path.join(folder_path, "lesson_plan.pdf")
                    if os.path.exists(pdf_path):
                        process_pdf(pdf_path, output_dir, "lesson_plan")
                if item.get("has_lesson_slide_pdf"):
                    pdf_path = os.path.join(folder_path, "lesson_slide.pdf")
                    if os.path.exists(pdf_path):
                        process_pdf(pdf_path, output_dir, "lesson_slide")
    walk(data)

# ==== 以下为从 smartcn_resource_download.py 移植的相关代码 ====
Base = declarative_base()

class ResourceProcessStatus(Base):
    __tablename__ = 'resource_process_status'
    course_bag_id = Column(String, primary_key=True)
    textbook_id = Column(String)
    lesson_plan = Column(Integer, default=0)
    video = Column(Integer, default=0)
    slides = Column(Integer, default=0)
    learning_task = Column(Integer, default=0)
    worksheet = Column(Integer, default=0)

    @staticmethod
    def get_all_course_bag_ids():
        """
        获取所有已存在的 course_bag_id
        """
        output_dir = os.path.join(os.path.dirname(__file__), '../temp_output/smartcn')
        db_path = os.path.join(output_dir, 'textbooks.db')
        Session = init_db(db_path)
        session = Session()
        ids = [row.course_bag_id for row in session.query(ResourceProcessStatus.course_bag_id).all()]
        session.close()
        return ids

def init_db(db_path):
    engine = create_engine(f'sqlite:///{db_path}')
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session

def get_unprocessed_lesson_plan_rows():
    """
    获取所有resource_download_status中lesson_plan>0的行，且resource_process_status中course_bag_id相同且lesson_plan<resource_download_status.lesson_plan的数据
    还包括resource_download_status中lesson_plan>0，且在resource_process_status不存在的行
    返回列表，每项为dict，包含course_bag_id, textbook_id, lesson_plan_downloaded, lesson_plan_processed
    """
    output_dir = os.path.join(os.path.dirname(__file__), '../temp_output/smartcn')
    db_path = os.path.join(output_dir, 'textbooks.db')
    Session = init_db(db_path)
    session = Session()
    RPS = aliased(ResourceProcessStatus)
    RDS = aliased(ResourceDownloadStatus)
    # 查询1: lesson_plan>0 且 resource_process_status 存在且 lesson_plan_processed < lesson_plan_downloaded
    q1 = session.query(
        RDS.course_bag_id,
        RDS.textbook_id,
        RDS.lesson_plan.label('lesson_plan_downloaded'),
        func.coalesce(RPS.lesson_plan, 0).label('lesson_plan_processed')
    ).outerjoin(
        RPS, RDS.course_bag_id == RPS.course_bag_id
    ).filter(
        RDS.lesson_plan > 0,
        func.coalesce(RPS.lesson_plan, 0) < RDS.lesson_plan
    )
    # 查询2: lesson_plan>0 且 resource_process_status 不存在
    q2 = session.query(
        RDS.course_bag_id,
        RDS.textbook_id,
        RDS.lesson_plan.label('lesson_plan_downloaded'),
        func.coalesce(RPS.lesson_plan, 0).label('lesson_plan_processed')
    ).outerjoin(
        RPS, RDS.course_bag_id == RPS.course_bag_id
    ).filter(
        RDS.lesson_plan > 0,
        RPS.course_bag_id == None
    )
    # 合并结果
    result = []
    for row in q1.all():
        result.append({
            "course_bag_id": row.course_bag_id,
            "textbook_id": row.textbook_id,
            "lesson_plan_downloaded": row.lesson_plan_downloaded,
            "lesson_plan_processed": row.lesson_plan_processed
        })
    for row in q2.all():
        result.append({
            "course_bag_id": row.course_bag_id,
            "textbook_id": row.textbook_id,
            "lesson_plan_downloaded": row.lesson_plan_downloaded,
            "lesson_plan_processed": row.lesson_plan_processed
        })
    session.close()
    return result

def process_unprocessed_lesson_plans():
    """
    遍历所有需要处理的course_bag_id，处理downloads下的lesson_plan pdf，输出到processed目录，并更新数据库
    """
    rows = get_unprocessed_lesson_plan_rows()
    if not rows:
        print("没有需要处理的lesson_plan pdf。")
        return

    output_dir = os.path.join(os.path.dirname(__file__), '../temp_output/smartcn')
    db_path = os.path.join(output_dir, 'textbooks.db')
    Session = init_db(db_path)
    session = Session()

    # 新增统计总数
    total_to_process = 0
    for row in rows:
        course_bag_id = row["course_bag_id"]
        download_dir = os.path.join(output_dir, "downloads", course_bag_id, "lesson_plan")
        if not os.path.exists(download_dir):
            continue
        pdf_files = [f for f in os.listdir(download_dir) if f.lower().endswith('.pdf')]
        total_to_process += len(pdf_files)

    print(f"需要处理的lesson_plan pdf总数: {total_to_process}")

    total_processed = 0
    for row in rows:
        course_bag_id = row["course_bag_id"]
        download_dir = os.path.join(output_dir, "downloads", course_bag_id, "lesson_plan")
        processed_dir = os.path.join(output_dir, "processed", course_bag_id, "lesson_plan")
        if not os.path.exists(download_dir):
            print(f"下载目录不存在: {download_dir}")
            continue
        pdf_files = [f for f in os.listdir(download_dir) if f.lower().endswith('.pdf')]
        pdf_files.sort()
        if not pdf_files:
            print(f"未找到pdf文件: {download_dir}")
            continue
        count = 0
        for idx, pdf_file in enumerate(pdf_files, 1):
            pdf_path = os.path.join(download_dir, pdf_file)
            os.makedirs(processed_dir, exist_ok=True)
            pdf_tag = os.path.splitext(pdf_file)[0]
            process_pdf(pdf_path, processed_dir, pdf_tag)
            # 更新数据库
            rps = session.query(ResourceProcessStatus).filter_by(course_bag_id=course_bag_id).first()
            if rps is None:
                rps = ResourceProcessStatus(
                    course_bag_id=course_bag_id,
                    textbook_id=row.get("textbook_id"),
                    lesson_plan=1
                )
                session.add(rps)
            else:
                rps.lesson_plan = (rps.lesson_plan or 0) + 1
            session.commit()
            count += 1
            total_processed += 1
            print(f"[{total_processed}/{total_to_process}] 已处理: {pdf_path} -> {processed_dir}")
    print(f"全部lesson_plan pdf处理完成，总计: {total_processed} 个。")
    session.close()

if __name__ == "__main__":
    # pdf_path = "/home/xwxsee/projects/ai-content-generation/temp_input/k12/single_pdf_process/2020QJ08SXRJ005_practice.pdf"
    process_unprocessed_lesson_plans()
