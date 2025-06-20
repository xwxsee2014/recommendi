import os
import json
import hashlib

from process_pdf import ResourceProcessStatus, init_db
from smartcn_resource_download import TextbookTM
from query_generator import CorpusQuery

RESOURCE_CATEGORY_CONFIG = {
    'lesson_plan': {
        'processed_dir': lambda output_dir, course_bag_id: os.path.join(output_dir, "processed", course_bag_id, "lesson_plan"),
        'ir_dataset_dir': lambda output_dir: os.path.join(output_dir, 'ir_datasets_splitted', 'lesson_plan'),
        'db_rows': lambda session: session.query(ResourceProcessStatus.course_bag_id).filter(ResourceProcessStatus.lesson_plan > 0).all(),
        'get_course_bag_id': lambda row: row.course_bag_id,
        'get_tag_names': lambda row: None
    },
    'tm_textbook': {
        'processed_dir': lambda output_dir, course_bag_id: os.path.join(output_dir, "tm_processed", course_bag_id),
        'ir_dataset_dir': lambda output_dir: os.path.join(output_dir, 'ir_datasets_splitted', 'tm_textbook'),
        'db_rows': lambda session: session.query(TextbookTM.id, TextbookTM.tag_names).filter(TextbookTM.processed > 0).all(),
        'get_course_bag_id': lambda row: row.id,
        'get_tag_names': lambda row: row.tag_names
    }
}

def build_resource_ir_dataset_corpus(resource_category):
    output_dir = os.path.join(os.path.dirname(__file__), '../temp_output/smartcn')
    config = RESOURCE_CATEGORY_CONFIG[resource_category]
    ir_dataset_dir = config['ir_dataset_dir'](output_dir)
    os.makedirs(ir_dataset_dir, exist_ok=True)
    db_path = os.path.join(output_dir, 'textbooks.db')
    print(f"Using database at {db_path}")
    Session = init_db(db_path)
    session = Session()

    rows = config['db_rows'](session)
    print(f"Found {len(rows)} resources for {resource_category}")
    docs = []
    para_types_set = set()
    span_types_set = set()
    for row in rows:
        course_bag_id = config['get_course_bag_id'](row)
        tag_names = config['get_tag_names'](row)
        processed_dir = config['processed_dir'](output_dir, course_bag_id)
        if not os.path.exists(processed_dir):
            continue
        # 针对 tm_textbook 获取 tag_names 字段
        for fname in os.listdir(processed_dir):
            if fname.lower().endswith('_middle.json'):
                file_stem = os.path.splitext(fname)[0]
                if not all(c.isalnum() or c in "_-" for c in file_stem):
                    file_stem_safe = hashlib.md5(file_stem.encode("utf-8")).hexdigest()
                else:
                    file_stem_safe = file_stem
                json_path = os.path.join(processed_dir, fname)
                with open(json_path, "r", encoding="utf-8") as f:
                    try:
                        data = json.load(f)
                    except Exception as e:
                        print(f"Failed to load {json_path}: {e}")
                        continue
                pdf_info = data.get("pdf_info", [])
                for page in pdf_info:
                    page_idx = page.get("page_idx")
                    para_blocks = page.get("para_blocks", [])
                    page_size = page.get("page_size", None)
                    for idx, para in enumerate(para_blocks):
                        # 收集 para_blocks 中 type
                        para_type = para.get("type")
                        if para_type is not None:
                            para_types_set.add(para_type)
                        bbox = para.get("bbox")
                        merged_contents = []
                        if para.get("type") == "table":
                            blocks = para.get("blocks", [])
                            for block in blocks:
                                block_content = []
                                block_lines = block.get("lines", [])
                                for line in block_lines:
                                    spans = line.get("spans", [])
                                    if not spans:
                                        continue
                                    for span in spans:
                                        # 收集 span 中 type
                                        span_type = span.get("type")
                                        if span_type is not None:
                                            span_types_set.add(span_type)
                                        if span.get("type") == "table":
                                            html = span.get("html", "")
                                            if html:
                                                block_content.append(html)
                                if block_content:
                                    merged_contents.append("".join(block_content))
                        else:
                            lines = para.get("lines", [])
                            for line in lines:
                                spans = line.get("spans", [])
                                if not spans:
                                    continue
                                line_content = []
                                for span in spans:
                                    # 收集 span 中 type
                                    span_type = span.get("type")
                                    if span_type is not None:
                                        span_types_set.add(span_type)
                                    if span.get("type") == "text":
                                        line_content.append(span.get("content", ""))
                                    elif span.get("type") == "inline_equation":
                                        line_content.append(f"${span.get('content', '')}$")
                                    elif span.get("type") == "interline_equation":
                                        line_content.append(f"$$\n{span.get('content', '')}\n$$")
                                    elif span.get("type") == "image":
                                        image_path = span.get("image_path", "")
                                        if image_path:
                                            line_content.append(f"[image]({image_path})")
                                if line_content:
                                    merged_contents.append("".join(line_content))
                        if not merged_contents:
                            continue
                        content = "".join(merged_contents)
                        doc = {
                            "doc_id": f"{course_bag_id}_{resource_category}_{file_stem_safe}_{page_idx}",
                            "page_idx": page_idx,
                            "bbox": bbox,
                            "bbox_index": idx,
                            "page_size": page_size,
                            "text": content
                        }
                        if tag_names is not None:
                            doc["tag_names"] = tag_names
                        docs.append(doc)
    # 打印 para_blocks 中 type 的种类
    print("para_blocks type set:", para_types_set)
    # 打印 span 中 type 的种类
    print("span type set:", span_types_set)
    session.close()
    out_path = os.path.join(ir_dataset_dir, "documents.jsonl")
    with open(out_path, "w", encoding="utf-8") as f:
        for doc in docs:
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")
    print(f"IR dataset written to {out_path}, total docs: {len(docs)}")
    return docs

def output_resource_merged_docs_jsonl(docs, resource_category):
    merged = {}
    tag_names_map = {}
    page_idx_map = {}
    for doc in docs:
        doc_id = doc["doc_id"]
        para_str = f"paragraph_{doc['bbox_index']}: {doc['text']}"
        if doc_id not in merged:
            merged[doc_id] = []
        merged[doc_id].append(para_str)
        # 如果包含 tag_names，记录
        if "tag_names" in doc:
            tag_names_map[doc_id] = doc["tag_names"]
        # 记录 page_idx
        if "page_idx" in doc:
            page_idx_map[doc_id] = doc["page_idx"]
    output_dir = RESOURCE_CATEGORY_CONFIG[resource_category]['ir_dataset_dir'](
        os.path.join(os.path.dirname(__file__), '../temp_output/smartcn')
    )
    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, "documents_merged.jsonl")
    with open(out_path, "w", encoding="utf-8") as f:
        for doc_id, paragraphs in merged.items():
            obj = {
                "doc_id": doc_id,
                "paragraphs": paragraphs
            }
            # 如果有 tag_names，写入
            if doc_id in tag_names_map:
                obj["tag_names"] = tag_names_map[doc_id]
            # 写入 page_idx
            if doc_id in page_idx_map:
                obj["page_idx"] = page_idx_map[doc_id]
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
    print(f"Merged IR dataset written to {out_path}, total docs: {len(merged)}")

def build_resource_ir_dataset_query(resource_category):
    output_dir = os.path.join(os.path.dirname(__file__), '../temp_output/smartcn')
    config = RESOURCE_CATEGORY_CONFIG[resource_category]
    ir_dataset_dir = config['ir_dataset_dir'](output_dir)
    os.makedirs(ir_dataset_dir, exist_ok=True)
    db_path = os.path.join(output_dir, 'textbooks.db')
    print(f"Using database at {db_path}")
    Session = init_db(db_path)
    session = Session()

    rows = session.query(CorpusQuery).filter(
        CorpusQuery.is_generated == 1,
        CorpusQuery.corpus_type == resource_category
    ).all()
    print(f"Found {len(rows)} corpus queries with is_generated=1")
    queries = []
    for row in rows:
        corpus_id = row.corpus_id
        queries_dir = os.path.join(output_dir, "queries")
        json_path = os.path.join(queries_dir, f"{corpus_id}.json")
        if not os.path.exists(json_path):
            continue
        with open(json_path, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except Exception as e:
                print(f"Failed to load {json_path}: {e}")
                continue
        queries_list = data.get("content", {}).get("queries", [])
        for idx, q in enumerate(queries_list):
            query_id = f"{corpus_id}_{idx}"
            queries.append({
                "query_id": query_id,
                "text": q.get("query", "")
            })                
    session.close()
    out_path = os.path.join(ir_dataset_dir, "queries.jsonl")
    with open(out_path, "w", encoding="utf-8") as f:
        for q in queries:
            f.write(json.dumps(q, ensure_ascii=False) + "\n")
    print(f"Query dataset written to {out_path}, total queries: {len(queries)}")

def build_resource_ir_dataset_qrel(resource_category):
    output_dir = os.path.join(os.path.dirname(__file__), '../temp_output/smartcn')
    config = RESOURCE_CATEGORY_CONFIG[resource_category]
    ir_dataset_dir = config['ir_dataset_dir'](output_dir)
    db_path = os.path.join(output_dir, 'textbooks.db')
    Session = init_db(db_path)
    session = Session()

    rows = session.query(CorpusQuery).filter(
        CorpusQuery.is_generated == 1,
        CorpusQuery.corpus_type == resource_category
    ).all()
    print(f"Found {len(rows)} corpus queries with is_generated=1")
    qrels = []
    for row in rows:
        corpus_id = row.corpus_id
        queries_dir = os.path.join(output_dir, "queries")
        json_path = os.path.join(queries_dir, f"{corpus_id}.json")
        if not os.path.exists(json_path):
            continue
        with open(json_path, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except Exception as e:
                print(f"Failed to load {json_path}: {e}")
                continue
        queries_list = data.get("content", {}).get("queries", [])
        for idx, q in enumerate(queries_list):
            query_id = f"{corpus_id}_{idx}"
            # 解析 recallable_paragraphs，提取所有 paragraph_{number} 的 number
            paragraph_indices = []
            recallable_paragraphs = q.get("recallable_paragraphs", [])
            for para in recallable_paragraphs:
                if isinstance(para, str) and para.startswith("paragraph_"):
                    try:
                        number = int(para.split("_")[1])
                        paragraph_indices.append(number)
                    except Exception:
                        continue
            # 构造 docs 列表
            docs_list = [{"doc_id": f"{corpus_id}_{number}", "relevance": 1} for number in paragraph_indices]
            qrels.append({
                "query_id": query_id,
                "docs": docs_list
            })
    session.close()
    out_path = os.path.join(ir_dataset_dir, "qrels.jsonl")
    with open(out_path, "w", encoding="utf-8") as f:
        for qrel in qrels:
            f.write(json.dumps(qrel, ensure_ascii=False) + "\n")
    print(f"Qrels written to {out_path}, total qrels: {len(qrels)}")

if __name__ == "__main__":
    # docs = build_resource_ir_dataset_corpus('tm_textbook')
    # output_resource_merged_docs_jsonl(docs, 'tm_textbook')
    # docs = build_resource_ir_dataset_corpus('lesson_plan')
    # output_resource_merged_docs_jsonl(docs, 'lesson_plan')
    build_resource_ir_dataset_query('tm_textbook')
    build_resource_ir_dataset_query('lesson_plan')
    build_resource_ir_dataset_qrel('tm_textbook')
    build_resource_ir_dataset_qrel('lesson_plan')
