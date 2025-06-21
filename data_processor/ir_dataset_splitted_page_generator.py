import os
import json
import hashlib

from process_pdf import ResourceProcessStatus, init_db
from smartcn_resource_download import TextbookTM, LessonPlanResourceMeta
from query_generator import CorpusQuery

RESOURCE_CATEGORY_CONFIG = {
    'lesson_plan': {
        'processed_dir': lambda output_dir, course_bag_id: os.path.join(output_dir, "processed", course_bag_id, "lesson_plan"),
        'ir_dataset_dir': lambda output_dir: os.path.join(output_dir, 'ir_datasets_splitted_page', 'lesson_plan'),
        'db_rows': lambda session: session.query(ResourceProcessStatus.course_bag_id).filter(ResourceProcessStatus.lesson_plan > 0).all(),
        'get_course_bag_id': lambda row: row.course_bag_id,
        'get_tag_names': lambda row: None,
        # 固化 meta_map 的生成
        'get_meta_map': lambda session: {
            (meta.course_bag_id, meta.filename): meta
            for meta in session.query(LessonPlanResourceMeta).all()
        }
    },
    'tm_textbook': {
        'processed_dir': lambda output_dir, course_bag_id: os.path.join(output_dir, "tm_processed", course_bag_id),
        'ir_dataset_dir': lambda output_dir: os.path.join(output_dir, 'ir_datasets_splitted_page', 'tm_textbook'),
        'db_rows': lambda session: session.query(TextbookTM.id, TextbookTM.tag_names).filter(TextbookTM.processed > 0).all(),
        'get_course_bag_id': lambda row: row.id,
        'get_tag_names': lambda row: row.tag_names,
        # 新增 meta_map 获取逻辑，key 为 (id, filename)
        'get_meta_map': lambda session: {
            (meta.id, meta.filename): meta
            for meta in session.query(TextbookTM).all()
            if meta.filename is not None
        }
    }
}

def load_meta_map(resource_category, session):
    config = RESOURCE_CATEGORY_CONFIG[resource_category]
    return config['get_meta_map'](session) if 'get_meta_map' in config else None

def process_row_files(row, config, output_dir, meta_map, resource_category, docs, para_types_set, span_types_set):
    course_bag_id = config['get_course_bag_id'](row)
    tag_names = config['get_tag_names'](row)
    processed_dir = config['processed_dir'](output_dir, course_bag_id)
    if not os.path.exists(processed_dir):
        return
    for fname in os.listdir(processed_dir):
        pdf_filename_stem = ""
        if fname.lower().endswith('_middle.json'):
            file_stem = os.path.splitext(fname)[0]
            pdf_filename_stem = file_stem.replace('_middle', '')
            json_path = os.path.join(processed_dir, fname)
            with open(json_path, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f)
                except Exception as e:
                    print(f"Failed to load {json_path}: {e}")
                    return
            pdf_info = data.get("pdf_info", [])
            meta = lookup_meta(meta_map, resource_category, course_bag_id, pdf_filename_stem)
            if meta_map is not None and not meta:
                print(f"Meta not found for ({course_bag_id}, {pdf_filename_stem}.pdf), skipping.")
                return
            page_doc_map = {}
            for page in pdf_info:
                process_page(page, meta, tag_names, docs, para_types_set, span_types_set, page_doc_map)
            # 合并后的每页 doc 加入 docs
            docs.extend(page_doc_map.values())

def lookup_meta(meta_map, resource_category, course_bag_id, pdf_filename_stem):
    pdf_filename = f"{pdf_filename_stem}.pdf"
    if resource_category == "lesson_plan":
        return meta_map.get((course_bag_id, pdf_filename))
    elif resource_category == "tm_textbook":
        return meta_map.get((course_bag_id, pdf_filename))
    return None

def process_page(page, meta, tag_names, docs, para_types_set, span_types_set, page_doc_map):
    page_idx = page.get("page_idx")
    para_blocks = page.get("para_blocks", [])
    page_size = page.get("page_size", None)
    merged_contents = []
    for idx, para in enumerate(para_blocks):
        para_type = para.get("type")
        if para_type is not None:
            para_types_set.add(para_type)
        merged_para_contents = extract_para_content(para, span_types_set)
        if not merged_para_contents:
            continue
        merged_contents.extend(merged_para_contents)
    if not merged_contents:
        return
    content = "\n".join(merged_contents)
    doc = construct_doc(meta, page_idx, page_size, content, tag_names)
    # 合并到 page_doc_map
    if page_idx not in page_doc_map:
        page_doc_map[page_idx] = doc
    else:
        # 合并内容
        page_doc_map[page_idx]["text"] += "\n" + doc["text"]

def extract_para_content(para, span_types_set):
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
    return merged_contents

def construct_doc(meta, page_idx, page_size, content, tag_names):
    if meta is not None:
        doc_id = f"{meta.id}_{page_idx}"
        doc = {
            "doc_id": doc_id,
            "page_id": doc_id,
            "id": meta.id,
            "resource_type_code": getattr(meta, "resource_type_code", None),
            "resource_type_code_name": getattr(meta, "resource_type_code_name", None),
            "container_id": getattr(meta, "container_id", None),
            "tag_list": getattr(meta, "tag_list", None),
            "parent_id": getattr(meta, "course_bag_id", None) if hasattr(meta, "course_bag_id") else None,
            "page_idx": page_idx,
            "page_size": page_size,
            "text": content
        }
    else:
        doc = {
            "doc_id": doc_id,
            "page_idx": page_idx,
            "page_size": page_size,
            "text": content
        }
    if tag_names is not None:
        doc["tag_names"] = tag_names
    return doc

def build_resource_ir_dataset_corpus(resource_category):
    output_dir = os.path.join(os.path.dirname(__file__), '../temp_output/smartcn')
    config = RESOURCE_CATEGORY_CONFIG[resource_category]
    ir_dataset_dir = config['ir_dataset_dir'](output_dir)
    os.makedirs(ir_dataset_dir, exist_ok=True)
    db_path = os.path.join(output_dir, 'textbooks.db')
    print(f"Using database at {db_path}")
    Session = init_db(db_path)
    session = Session()
    meta_map = load_meta_map(resource_category, session)
    rows = config['db_rows'](session)
    print(f"Found {len(rows)} resources for {resource_category}")
    docs = []
    para_types_set = set()
    span_types_set = set()
    for row in rows:
        process_row_files(row, config, output_dir, meta_map, resource_category, docs, para_types_set, span_types_set)
    print("para_blocks type set:", para_types_set)
    print("span type set:", span_types_set)
    session.close()
    out_path = os.path.join(ir_dataset_dir, "documents.jsonl")
    with open(out_path, "w", encoding="utf-8") as f:
        for doc in docs:
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")
    print(f"IR dataset written to {out_path}, total docs: {len(docs)}")
    return docs

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

def regenerate_lesson_plan_queries_and_qrels_with_new_ids():
    """
    重新生成 lesson_plan 的 queries.jsonl 和 qrels.jsonl，变换 query_id 和 doc_id，并生成 queries_mapping.jsonl。
    """
    output_dir = os.path.join(os.path.dirname(__file__), '../temp_output/smartcn')
    ir_dataset_dir = os.path.join(output_dir, 'ir_datasets_splitted', 'lesson_plan')
    db_path = os.path.join(output_dir, 'textbooks.db')
    queries_path = os.path.join(ir_dataset_dir, "queries.jsonl")
    qrels_path = os.path.join(ir_dataset_dir, "qrels.jsonl")
    mapping_path = os.path.join(ir_dataset_dir, "queries_mapping.jsonl")

    # 1. 加载 LessonPlanResourceMeta
    Session = init_db(db_path)
    session = Session()
    meta_map = {}
    for meta in session.query(LessonPlanResourceMeta).all():
        meta_map[(meta.course_bag_id, meta.filename_code)] = meta
    session.close()

    # 2. 读取 queries.jsonl，生成 mapping
    mapping_list = []
    old_to_new_queryid = {}
    with open(queries_path, "r", encoding="utf-8") as f:
        queries = [json.loads(line) for line in f]
    new_queries = []
    for q in queries:
        old_query_id = q["query_id"]
        # 解析 old_query_id: {course_bag_id}_{resource_category}_{filename_code}_{page_idx}_{query_idx}
        parts = old_query_id.split("_")
        if len(parts) < 6:
            print(f"Invalid query_id format: {old_query_id}")
            exit(1)
        course_bag_id = parts[0]
        filename_code = "_".join(parts[3:-2])
        page_idx = parts[-2]
        query_idx = parts[-1]
        meta_key = (course_bag_id, filename_code)
        meta = meta_map.get(meta_key)
        if not meta:
            print(f"Meta not found for ({course_bag_id}, {filename_code}), abort.")
            exit(1)
        new_query_id = f"{meta.id}_{page_idx}_{query_idx}"
        mapping = {
            "old_query_id": old_query_id,
            "new_query_id": new_query_id,
            "doc_id": meta.id,
            "page_idx": page_idx
        }
        mapping_list.append(mapping)
        old_to_new_queryid[old_query_id] = mapping
        new_q = dict(q)
        new_q["query_id"] = new_query_id
        new_queries.append(new_q)
    # 写入 mapping
    with open(mapping_path, "w", encoding="utf-8") as f:
        for m in mapping_list:
            f.write(json.dumps(m, ensure_ascii=False) + "\n")
    print(f"Mapping written to {mapping_path}, total: {len(mapping_list)}")

    # 写入新的 queries.jsonl
    new_queries_path = os.path.join(ir_dataset_dir, "queries.jsonl")
    with open(new_queries_path, "w", encoding="utf-8") as f:
        for q in new_queries:
            f.write(json.dumps(q, ensure_ascii=False) + "\n")
    print(f"New queries.jsonl written to {new_queries_path}")

    # 3. 替换 qrels.jsonl 中的 doc_id
    with open(qrels_path, "r", encoding="utf-8") as f:
        qrels = [json.loads(line) for line in f]
    new_qrels = []
    for qrel in qrels:
        old_query_id = qrel["query_id"]
        mapping = old_to_new_queryid.get(old_query_id)
        new_query_id = mapping["new_query_id"]
        new_docs = []
        for doc in qrel["docs"]:
            doc_parts = doc["doc_id"].split("_")
            para_idx = doc_parts[-1]
            new_doc_id = f"{mapping['doc_id']}_{mapping['page_idx']}_{para_idx}"
            new_docs.append({
                "doc_id": new_doc_id,
                "relevance": doc["relevance"]
            })
        new_qrels.append({
            "query_id": new_query_id,
            "docs": new_docs
        })
    # 写入新的 qrels.jsonl
    new_qrels_path = os.path.join(ir_dataset_dir, "qrels.jsonl")
    with open(new_qrels_path, "w", encoding="utf-8") as f:
        for qrel in new_qrels:
            f.write(json.dumps(qrel, ensure_ascii=False) + "\n")
    print(f"New qrels.jsonl written to {new_qrels_path}")

def regenerate_tm_textbook_queries_and_qrels_with_new_ids():
    """
    重新生成 tm_textbook 的 queries.jsonl 和 qrels.jsonl，变换 query_id 和 doc_id，并生成 queries_mapping.jsonl。
    """
    output_dir = os.path.join(os.path.dirname(__file__), '../temp_output/smartcn')
    ir_dataset_dir = os.path.join(output_dir, 'ir_datasets_splitted', 'tm_textbook')
    db_path = os.path.join(output_dir, 'textbooks.db')
    queries_path = os.path.join(ir_dataset_dir, "queries.jsonl")
    qrels_path = os.path.join(ir_dataset_dir, "qrels.jsonl")
    mapping_path = os.path.join(ir_dataset_dir, "queries_mapping.jsonl")

    # 1. 加载 TextbookTM
    Session = init_db(db_path)
    session = Session()
    meta_map = {}
    for meta in session.query(TextbookTM).all():
        meta_map[(meta.id, meta.filename_code)] = meta
    session.close()

    # 2. 读取 queries.jsonl，生成 mapping
    mapping_list = []
    old_to_new_queryid = {}
    with open(queries_path, "r", encoding="utf-8") as f:
        queries = [json.loads(line) for line in f]
    new_queries = []
    for q in queries:
        old_query_id = q["query_id"]
        # 解析 old_query_id: {id}_{resource_category}_{filename_code}_{page_idx}_{query_idx}
        parts = old_query_id.split("_")
        if len(parts) < 6:
            print(f"Invalid query_id format: {old_query_id}")
            exit(1)
        id_ = parts[0]
        filename_code = "_".join(parts[3:-2])
        page_idx = parts[-2]
        query_idx = parts[-1]
        meta_key = (id_, filename_code)
        meta = meta_map.get(meta_key)
        if not meta:
            print(f"Meta not found for ({id_}, {filename_code}), abort.")
            exit(1)
        new_query_id = f"{meta.id}_{page_idx}_{query_idx}"
        mapping = {
            "old_query_id": old_query_id,
            "new_query_id": new_query_id,
            "doc_id": meta.id,
            "page_idx": page_idx
        }
        mapping_list.append(mapping)
        old_to_new_queryid[old_query_id] = mapping
        new_q = dict(q)
        new_q["query_id"] = new_query_id
        new_queries.append(new_q)
    # 写入 mapping
    with open(mapping_path, "w", encoding="utf-8") as f:
        for m in mapping_list:
            f.write(json.dumps(m, ensure_ascii=False) + "\n")
    print(f"Mapping written to {mapping_path}, total: {len(mapping_list)}")

    # 写入新的 queries.jsonl
    new_queries_path = os.path.join(ir_dataset_dir, "queries.jsonl")
    with open(new_queries_path, "w", encoding="utf-8") as f:
        for q in new_queries:
            f.write(json.dumps(q, ensure_ascii=False) + "\n")
    print(f"New queries.jsonl written to {new_queries_path}")

    # 3. 替换 qrels.jsonl 中的 doc_id
    with open(qrels_path, "r", encoding="utf-8") as f:
        qrels = [json.loads(line) for line in f]
    new_qrels = []
    for qrel in qrels:
        old_query_id = qrel["query_id"]
        mapping = old_to_new_queryid.get(old_query_id)
        new_query_id = mapping["new_query_id"]
        new_docs = []
        for doc in qrel["docs"]:
            doc_parts = doc["doc_id"].split("_")
            para_idx = doc_parts[-1]
            new_doc_id = f"{mapping['doc_id']}_{mapping['page_idx']}_{para_idx}"
            new_docs.append({
                "doc_id": new_doc_id,
                "relevance": doc["relevance"]
            })
        new_qrels.append({
            "query_id": new_query_id,
            "docs": new_docs
        })
    # 写入新的 qrels.jsonl
    new_qrels_path = os.path.join(ir_dataset_dir, "qrels.jsonl")
    with open(new_qrels_path, "w", encoding="utf-8") as f:
        for qrel in new_qrels:
            f.write(json.dumps(qrel, ensure_ascii=False) + "\n")
    print(f"New qrels.jsonl written to {new_qrels_path}")

if __name__ == "__main__":
    docs = build_resource_ir_dataset_corpus('tm_textbook')
    docs = build_resource_ir_dataset_corpus('lesson_plan')
    # build_resource_ir_dataset_query('tm_textbook')
    # build_resource_ir_dataset_query('lesson_plan')
    # build_resource_ir_dataset_qrel('tm_textbook')
    # build_resource_ir_dataset_qrel('lesson_plan')
