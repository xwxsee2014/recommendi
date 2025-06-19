import os
from sqlalchemy import create_engine, Column, String, Integer, and_, Boolean, func, text
from sqlalchemy.orm import declarative_base, sessionmaker
import json
import random
from utils import LOG
import time
import requests

Base = declarative_base()

class CorpusQuery(Base):
    __tablename__ = 'corpus_query'
    corpus_id = Column(String, primary_key=True)
    corpus_type = Column(String, primary_key=True)
    is_generated = Column(Integer, default=0)

def init_db(db_path):
    engine = create_engine(f'sqlite:///{db_path}')
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session

def save_corpus_query(db_path, corpus_id, corpus_type):
    """
    保存 corpus_id, corpus_type, is_generated=1 到数据库
    """
    Session = init_db(db_path)
    session = Session()
    obj = CorpusQuery(corpus_id=corpus_id, corpus_type=corpus_type, is_generated=1)
    session.merge(obj)
    session.commit()
    session.close()


LESSON_PLAN_SAMPLE_RATIO = 0.1
TM_TEXTBOOK_TAGS = [
    "艺术·舞蹈", "英语", "体育与健康", "艺术·音乐", "艺术·美术", "物理", "化学", "生物学",
    "历史", "地理", "数学", "语文", "思想政治", "道德与法治", "通用技术", "艺术"
]
TM_TEXTBOOK_GRADES = ["小学", "初中", "高中"]
TM_TEXTBOOK_COMBINATIONS = [
    f"{tag}_{grade}" for tag in TM_TEXTBOOK_TAGS for grade in TM_TEXTBOOK_GRADES
]
TM_TEXTBOOK_SAMPLE_SIZE = 10  # 每个 tag_name 最多采样 20 个文档
API_URL = "http://localhost/v1/completion-messages"
API_HEADERS = {
    "Authorization": "Bearer app-dsnt7dkpytfNRiIdvF5ALi4k",
    "Content-Type": "application/json"
}
API_USER = "xwxsee"
QUERIES_DIR = os.path.join(os.path.dirname(__file__), '../temp_output/smartcn/queries')

def extract_content_from_answer(answer):
    import re
    # 匹配 answer 字符串任意位置的 ```json {content} ``` 或 ``` {content} ```
    patterns = [
        r"```json\s*({.*?})\s*```",
        r"```\s*({.*?})\s*```",
        r"({.*})"
    ]
    for pat in patterns:
        for match in re.finditer(pat, answer, re.DOTALL):
            try:
                return json.loads(match.group(1))
            except Exception as e:
                LOG.warning(f"Failed to parse JSON from match: {match.group(1)} - {e}")
                continue
    return None

def sample_documents(resource_category, documents):
    sampled_docs = []
    if resource_category == 'lesson_plan':
        sample_size = max(1, int(len(documents) * LESSON_PLAN_SAMPLE_RATIO))
        sampled_docs = random.sample(documents, sample_size)
        LOG.info(f"Sampled {len(sampled_docs)} documents for lesson_plan")
    elif resource_category == 'tm_textbook':
        tag_names_set = set()
        tag_name_to_docs = {}
        for doc in documents:
            tag_name = doc.get('tag_names')
            if isinstance(tag_name, list):
                tag_name = ','.join(tag_name)
            tag_names_set.add(tag_name)
            tag_name_to_docs.setdefault(tag_name, []).append(doc)
        for tag in TM_TEXTBOOK_TAGS:
            for grade in TM_TEXTBOOK_GRADES:
                matched_tag_names = [tn for tn in tag_names_set if tag in tn and grade in tn]
                if not matched_tag_names:
                    continue
                sampled_tag_names = random.sample(matched_tag_names, min(2, len(matched_tag_names)))
                for tn in sampled_tag_names:
                    docs = [d for d in tag_name_to_docs[tn] if d.get('page_idx', 0) > 2]
                    if docs:
                        sampled = random.sample(docs, min(TM_TEXTBOOK_SAMPLE_SIZE, len(docs)))
                        sampled_docs.extend(sampled)
        LOG.info(f"Sampled {len(sampled_docs)} documents for tm_textbook")
    else:
        LOG.warning(f"Unknown resource_category: {resource_category}")
    return sampled_docs

def sync_corpus_query(session, sampled_docs, resource_category):
    sampled_doc_ids = set(doc.get('doc_id') for doc in sampled_docs if doc.get('doc_id'))
    existing_query_ids = set(
        r[0] for r in session.query(CorpusQuery.corpus_id)
        .filter(CorpusQuery.corpus_type == resource_category)
        .all()
    )
    new_doc_ids = sampled_doc_ids - existing_query_ids
    for doc_id in new_doc_ids:
        obj = CorpusQuery(corpus_id=doc_id, corpus_type=resource_category, is_generated=0)
        session.merge(obj)
    session.commit()

def get_docs_to_generate(session, documents, sampled_docs, resource_category, limit):
    total_query_count = session.query(CorpusQuery).filter(CorpusQuery.corpus_type == resource_category).count()
    not_generated_records = session.query(CorpusQuery).filter(
        CorpusQuery.corpus_type == resource_category,
        CorpusQuery.is_generated == 0
    ).all()
    not_generated_count = len(not_generated_records)
    docs_to_generate = []
    if limit <= total_query_count:
        if not_generated_count >= limit:
            selected_records = not_generated_records[:limit]
        else:
            selected_records = not_generated_records
        doc_id_set = set(r.corpus_id for r in selected_records)
        docs_to_generate = [doc for doc in documents if doc.get('doc_id') in doc_id_set]
    else:
        doc_id_set = set(r.corpus_id for r in not_generated_records)
        docs_to_generate = [doc for doc in documents if doc.get('doc_id') in doc_id_set]
        need_more = limit - len(docs_to_generate)
        generated_ids = set(
            r[0] for r in session.query(CorpusQuery.corpus_id)
            .filter(CorpusQuery.corpus_type == resource_category, CorpusQuery.is_generated == 1)
            .all()
        )
        remaining_docs = [doc for doc in sampled_docs if doc.get('doc_id') not in generated_ids and doc.get('doc_id') not in doc_id_set]
        if need_more > 0 and remaining_docs:
            more_docs = random.sample(remaining_docs, min(need_more, len(remaining_docs)))
            docs_to_generate.extend(more_docs)
            for doc in more_docs:
                doc_id = doc.get('doc_id')
                if doc_id:
                    obj = CorpusQuery(corpus_id=doc_id, corpus_type=resource_category, is_generated=0)
                    session.merge(obj)
            session.commit()
    return docs_to_generate

def generate_and_save_queries(session, docs_to_generate, resource_category, db_path):
    total = len(docs_to_generate)
    LOG.info(f"Start processing {total} documents for {resource_category}")
    for idx, doc in enumerate(docs_to_generate, 1):
        doc_id = doc.get('doc_id')
        paragraphs = doc.get('paragraphs')
        if not doc_id or not paragraphs:
            LOG.warning(f"Skip doc with missing doc_id or paragraphs at index {idx}")
            continue
        exists = session.query(CorpusQuery).filter_by(corpus_id=doc_id, corpus_type=resource_category, is_generated=1).first()
        if exists:
            LOG.info(f"[{idx}/{total}] doc_id={doc_id} already generated, skip.")
            continue
        content = None
        for attempt in range(3):
            try:
                LOG.info(f"[{idx}/{total}] Requesting API for doc_id={doc_id}, attempt {attempt+1}")
                resp = requests.post(
                    API_URL,
                    headers=API_HEADERS,
                    json={
                        "inputs": {"query": json.dumps(paragraphs, ensure_ascii=False)},
                        "response_mode": "blocking",
                        "user": API_USER
                    },
                    timeout=60
                )
                if resp.status_code == 200:
                    data = resp.json()
                    answer = data.get("answer", "")
                    content = extract_content_from_answer(answer)
                    if content:
                        LOG.info(f"[{idx}/{total}] Successfully extracted content for doc_id={doc_id}")
                        break
                    else:
                        LOG.warning(f"[{idx}/{total}] doc_id={doc_id} content extraction failed, attempt {attempt+1}")
                else:
                    LOG.warning(f"[{idx}/{total}] doc_id={doc_id} API status {resp.status_code}, attempt {attempt+1}")
            except Exception as e:
                LOG.warning(f"[{idx}/{total}] doc_id={doc_id} API error: {e}, attempt {attempt+1}")
            time.sleep(1)
        if not content:
            LOG.warning(f"[{idx}/{total}] doc_id={doc_id} failed to get content after 3 attempts")
            continue
        out_path = os.path.join(QUERIES_DIR, f"{doc_id}.json")
        try:
            with open(out_path, 'w', encoding='utf-8') as fout:
                json.dump({"content": content}, fout, ensure_ascii=False, indent=2)
            LOG.info(f"[{idx}/{total}] Wrote content to {out_path}")
        except Exception as e:
            LOG.warning(f"[{idx}/{total}] Failed to write file {out_path}: {e}")
            continue
        try:
            save_corpus_query(db_path, doc_id, resource_category)
            LOG.info(f"[{idx}/{total}] Saved corpus_query for doc_id={doc_id}")
        except Exception as e:
            LOG.warning(f"[{idx}/{total}] Failed to save corpus_query for doc_id={doc_id}: {e}")

def process_existing_corpus_queries(session, doc_path, resource_category, limit, db_path):
    """
    检查 corpus_query 表中是否已满足 limit，如果满足则只处理未生成的记录
    返回 True 表示已处理，无需后续采样和插入
    """
    total_query_count = session.query(CorpusQuery).filter(CorpusQuery.corpus_type == resource_category).count()
    if total_query_count >= limit:
        not_generated_records = session.query(CorpusQuery).filter(
            CorpusQuery.corpus_type == resource_category,
            CorpusQuery.is_generated == 0
        ).all()
        if not not_generated_records:
            LOG.info(f"corpus_query already has {total_query_count} records for {resource_category}, all generated.")
            return True
        doc_id_set = set(r.corpus_id for r in not_generated_records[:limit])
        documents = []
        with open(doc_path, 'r', encoding='utf-8') as f:
            for line in f:
                doc = json.loads(line)
                if doc.get('doc_id') in doc_id_set:
                    documents.append(doc)
        generate_and_save_queries(session, documents, resource_category, db_path)
        LOG.info(f"Finished processing {len(documents)} documents for {resource_category}")
        return True
    return False

def generate_queries_for_resource_category(resource_category, limit=100):
    """
    读取 documents_merged.jsonl，采样，调用API生成queries，写入文件并记录数据库
    limit: 需要生成的query总数
    """
    base_dir = os.path.dirname(__file__)
    doc_path = os.path.join(base_dir, f'../temp_output/smartcn/ir_datasets_splitted/{resource_category}/documents_merged.jsonl')
    db_path = os.path.join(base_dir, '../temp_output/smartcn/textbooks.db')
    os.makedirs(QUERIES_DIR, exist_ok=True)
    Session = init_db(db_path)
    session = Session()

    # 先检查 corpus_query 表中是否已满足 limit
    if process_existing_corpus_queries(session, doc_path, resource_category, limit, db_path):
        session.close()
        return

    # 否则正常采样
    documents = []
    with open(doc_path, 'r', encoding='utf-8') as f:
        for line in f:
            doc = json.loads(line)
            documents.append(doc)
    LOG.info(f"Loaded {len(documents)} documents for resource_category={resource_category}")
    sampled_docs = sample_documents(resource_category, documents)
    if not sampled_docs:
        LOG.warning(f"No sampled docs for {resource_category}")
        session.close()
        return
    sync_corpus_query(session, sampled_docs, resource_category)
    docs_to_generate = get_docs_to_generate(session, documents, sampled_docs, resource_category, limit)
    generate_and_save_queries(session, docs_to_generate, resource_category, db_path)
    session.close()
    LOG.info(f"Finished processing {len(docs_to_generate)} documents for {resource_category}")


if __name__ == "__main__":
    # generate_queries_for_resource_category('lesson_plan', limit=200)
    generate_queries_for_resource_category('tm_textbook', limit=670)
