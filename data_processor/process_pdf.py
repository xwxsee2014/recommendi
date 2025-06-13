import os
import json

from magic_pdf.data.data_reader_writer import FileBasedDataWriter, FileBasedDataReader
from magic_pdf.data.dataset import PymuDocDataset
from magic_pdf.model.doc_analyze_by_custom_model import doc_analyze
from magic_pdf.config.enums import SupportedPdfParseMethod
from sqlalchemy import create_engine, Column, String, Integer, func
from sqlalchemy.orm import declarative_base, sessionmaker
from data_processor.smartcn_resource_download import ResourceDownloadStatus

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

def init_db(db_path):
    engine = create_engine(f'sqlite:///{db_path}')
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session

def get_unprocessed_lesson_plan_rows():
    """
    获取所有resource_download_status中lesson_plan>0的行，且resource_process_status中course_bag_id相同且lesson_plan<resource_download_status.lesson_plan的数据
    返回列表，每项为dict，包含course_bag_id, textbook_id, lesson_plan_downloaded, lesson_plan_processed
    """
    output_dir = os.path.join(os.path.dirname(__file__), '../temp_output/smartcn')
    db_path = os.path.join(output_dir, 'textbooks.db')
    Session = init_db(db_path)
    session = Session()
    from sqlalchemy.orm import aliased
    RPS = aliased(ResourceProcessStatus)
    RDS = aliased(ResourceDownloadStatus)
    q = session.query(
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
    result = []
    for row in q.all():
        result.append({
            "course_bag_id": row.course_bag_id,
            "textbook_id": row.textbook_id,
            "lesson_plan_downloaded": row.lesson_plan_downloaded,
            "lesson_plan_processed": row.lesson_plan_processed
        })
    session.close()
    return result

if __name__ == "__main__":
    # traverse_and_process()
    pdf_path = "/home/xwxsee/projects/ai-content-generation/temp_input/k12/single_pdf_process/2020QJ08SXRJ005_practice.pdf"
    process_pdf_from_path(pdf_path)
