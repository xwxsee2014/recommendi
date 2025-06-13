import os
import json

from magic_pdf.data.data_reader_writer import FileBasedDataWriter, FileBasedDataReader
from magic_pdf.data.dataset import PymuDocDataset
from magic_pdf.model.doc_analyze_by_custom_model import doc_analyze
from magic_pdf.config.enums import SupportedPdfParseMethod

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

if __name__ == "__main__":
    # traverse_and_process()
    pdf_path = "/home/xwxsee/projects/ai-content-generation/temp_input/k12/single_pdf_process/2020QJ08SXRJ005_practice.pdf"
    process_pdf_from_path(pdf_path)
