import os
import json

from process_pdf import ResourceProcessStatus, init_db
from query_generator import CorpusQuery

def build_lesson_plan_ir_dataset_corpus():
    output_dir = os.path.join(os.path.dirname(__file__), '../temp_output/smartcn')
    ir_dataset_dir = os.path.join(output_dir, 'ir_datasets', 'lesson_plan')
    os.makedirs(ir_dataset_dir, exist_ok=True)
    db_path = os.path.join(output_dir, 'textbooks.db')
    print(f"Using database at {db_path}")
    Session = init_db(db_path)
    session = Session()

    # Query for lesson_plan=1
    rows = session.query(ResourceProcessStatus.course_bag_id).filter(ResourceProcessStatus.lesson_plan > 0).all()
    print(f"Found {len(rows)} resources with lesson_plan > 0")
    docs = []
    for row in rows:
        course_bag_id = row.course_bag_id
        lesson_plan_dir = os.path.join(output_dir, "processed", course_bag_id, "lesson_plan")
        if not os.path.exists(lesson_plan_dir):
            continue
        for fname in os.listdir(lesson_plan_dir):
            if fname.lower().endswith('.md'):
                md_path = os.path.join(lesson_plan_dir, fname)
                with open(md_path, "r", encoding="utf-8") as f:
                    text = f.read()
                doc_id = f"{course_bag_id}_lesson_plan_{os.path.splitext(fname)[0]}"
                docs.append({
                    "doc_id": doc_id,
                    "text": text
                })
    session.close()
    # Output as JSONL
    out_path = os.path.join(ir_dataset_dir, "documents.jsonl")
    with open(out_path, "w", encoding="utf-8") as f:
        for doc in docs:
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")
    print(f"IR dataset written to {out_path}, total docs: {len(docs)}")


def build_lesson_plan_ir_dataset_query():
    output_dir = os.path.join(os.path.dirname(__file__), '../temp_output/smartcn')
    ir_dataset_dir = os.path.join(output_dir, 'ir_datasets', 'lesson_plan')
    os.makedirs(ir_dataset_dir, exist_ok=True)
    db_path = os.path.join(output_dir, 'textbooks.db')
    print(f"Using database at {db_path}")
    Session = init_db(db_path)
    session = Session()

    # 查询 CorpusQuery 表中 is_generated=1 的数据
    rows = session.query(CorpusQuery).filter(CorpusQuery.is_generated == 1).all()
    print(f"Found {len(rows)} corpus queries with is_generated=1")
    queries = []
    for row in rows:
        corpus_id = row.corpus_id
        corpus_type = row.corpus_type
        queries_dir = os.path.join(output_dir, "queries", corpus_id, corpus_type)
        if not os.path.exists(queries_dir):
            continue
        for fname in os.listdir(queries_dir):
            if fname.lower().endswith('.json'):
                json_path = os.path.join(queries_dir, fname)
                with open(json_path, "r", encoding="utf-8") as f:
                    try:
                        data = json.load(f)
                    except Exception as e:
                        print(f"Failed to load {json_path}: {e}")
                        continue
                queries_list = data.get("queries", [])
                file_stem = os.path.splitext(fname)[0]
                for idx, q in enumerate(queries_list):
                    query_id = f"{corpus_id}_{corpus_type}_{file_stem}_{idx}"
                    queries.append({
                        "query_id": query_id,
                        "text": q
                    })
    session.close()
    # Output as JSONL
    out_path = os.path.join(ir_dataset_dir, "queries.jsonl")
    with open(out_path, "w", encoding="utf-8") as f:
        for q in queries:
            f.write(json.dumps(q, ensure_ascii=False) + "\n")
    print(f"Query dataset written to {out_path}, total queries: {len(queries)}")


def build_lesson_plan_ir_dataset_qrel():
    output_dir = os.path.join(os.path.dirname(__file__), '../temp_output/smartcn')
    ir_dataset_dir = os.path.join(output_dir, 'ir_datasets', 'lesson_plan')
    queries_path = os.path.join(ir_dataset_dir, "queries.jsonl")
    qrels = []
    if not os.path.exists(queries_path):
        print(f"{queries_path} does not exist.")
        return
    with open(queries_path, "r", encoding="utf-8") as f:
        for line in f:
            item = json.loads(line)
            query_id = item.get("query_id")
            # doc_id 为 query_id 去掉最后一个下划线及其后的内容
            if query_id is None:
                continue
            if "_" not in query_id:
                continue
            doc_id = "_".join(query_id.split("_")[:-1])
            qrels.append({
                "query_id": query_id,
                "doc_id": doc_id,
                "relevance": 1
            })
    out_path = os.path.join(ir_dataset_dir, "qrels.jsonl")
    with open(out_path, "w", encoding="utf-8") as f:
        for qrel in qrels:
            f.write(json.dumps(qrel, ensure_ascii=False) + "\n")
    print(f"Qrels written to {out_path}, total qrels: {len(qrels)}")


if __name__ == "__main__":
    build_lesson_plan_ir_dataset_corpus()
    build_lesson_plan_ir_dataset_query()
    build_lesson_plan_ir_dataset_qrel()
