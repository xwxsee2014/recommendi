import requests
from typing import List, Dict
from tqdm import tqdm

from qdrant_client import QdrantClient
from qdrant_client.http.models import PointStruct

import ir_datasets

from sentence_transformers import SentenceTransformer

NF_DATASET = "beir/nfcorpus/test"
QDRANT_COLLECTION = "nfcorpus_docs"
QDRANT_HOST = "http://localhost:6333"

# 1. 加载nfcorpus数据
def load_nfcorpus():
    dataset = ir_datasets.load(NF_DATASET)
    # corpus: dict[str, dict], queries: dict[str, str], qrels: dict[str, dict[str, int]]
    corpus = {doc.doc_id: {"title": doc.title or "", "text": doc.text} for doc in dataset.docs_iter()}
    queries = {q.query_id: q.text for q in dataset.queries_iter()}
    qrels = {}
    for qrel in dataset.qrels_iter():
        if qrel.query_id not in qrels:
            qrels[qrel.query_id] = {}
        qrels[qrel.query_id][qrel.doc_id] = qrel.relevance

    # 转换为列表形式以兼容后续代码
    docs = [corpus[doc_id]["text"] for doc_id in corpus]
    doc_ids = list(corpus.keys())
    query_texts = [queries[qid] for qid in queries]
    query_ids = list(queries.keys())

    # qrels_dict: {query_id: [doc_id, ...]} 只保留相关性大于0的doc
    qrels_dict = {}
    for qid, doc_dict in qrels.items():
        qrels_dict[qid] = [did for did, rel in doc_dict.items() if rel > 0]

    print(len(docs), "documents loaded.")
    print(len(query_texts), "queries loaded.")
    print(len(qrels_dict), "queries with relevant documents.")

    return docs, doc_ids, query_texts, query_ids, qrels_dict

# 2. BM25召回
def bm25_recall(docs: List[str], queries: List[str], doc_ids: List[str], topk=10):
    client = QdrantClient(url=QDRANT_HOST)
    collection_name = QDRANT_COLLECTION + "_bm25"
    # 使用 collection_exists 和 create_collection 替代 recreate_collection
    if not client.collection_exists(collection_name=collection_name):
        client.create_collection(
            collection_name=collection_name,
            vectors_config=None,
            on_disk_payload=True
        )
        # BM25模式下，PointStruct 不能包含 vector 字段，需用 dict 形式插入
        points = [
            {"id": i, "payload": {"doc_id": doc_ids[i], "text": docs[i]}}
            for i in range(len(doc_ids))
        ]
        client.upsert(collection_name=collection_name, points=points)
    results = {}
    for idx, query in enumerate(tqdm(queries, desc="BM25-Qdrant")):
        hits = client.query_points(
            collection_name=collection_name,
            limit=topk,
            search_params={"exact": True, "bm25": {"query": query, "fields": ["text"]}},
            with_payload=True
        )
        retrieved = [hit.payload["doc_id"] for hit in hits]
        results[idx] = retrieved
    return results

# 3. 本地embedding召回
def embedding_recall_local(docs, queries, doc_ids, topk=10):
    model = SentenceTransformer("all-MiniLM-L6-v2")
    doc_embs = model.encode(docs, batch_size=64, show_progress_bar=True)
    query_embs = model.encode(queries, batch_size=64, show_progress_bar=True)
    # Qdrant入库
    client = QdrantClient(url=QDRANT_HOST)
    if not client.collection_exists(collection_name=QDRANT_COLLECTION):
        client.create_collection(
            collection_name=QDRANT_COLLECTION,
            vectors_config={"size": doc_embs.shape[1], "distance": "Cosine"}
        )
        points = [PointStruct(id=i, vector=doc_embs[i], payload={"doc_id": doc_ids[i]}) for i in range(len(doc_ids))]
        client.upsert(collection_name=QDRANT_COLLECTION, points=points)
    results = {}
    for idx, emb in enumerate(tqdm(query_embs, desc="Embedding-Local")):
        hits = client.search(collection_name=QDRANT_COLLECTION, query_vector=emb, limit=topk)
        retrieved = [hit.payload["doc_id"] for hit in hits]
        results[idx] = retrieved
    return results

# 4. 远程embedding召回（xinference）
def embedding_recall_xinference(docs, queries, doc_ids, topk=10):
    # xinference embedding
    def get_embedding(texts: List[str]) -> List[List[float]]:
        url = "http://localhost:9998/v1/embeddings"
        headers = {"Authorization": "Bearer sk-72tkvudyGLPMi"}
        data = {"model": "bge-m3", "input": texts}
        resp = requests.post(url, json=data, headers=headers)
        resp.raise_for_status()
        return resp.json()["data"]["embeddings"]
    # 文档embedding
    doc_embs = get_embedding(docs)
    query_embs = get_embedding(queries)
    # Qdrant入库
    client = QdrantClient(url=QDRANT_HOST)
    collection_name = QDRANT_COLLECTION + "_xinference"
    if not client.collection_exists(collection_name=collection_name):
        client.create_collection(
            collection_name=collection_name,
            vectors_config={"size": len(doc_embs[0]), "distance": "Cosine"}
        )
        points = [PointStruct(id=i, vector=doc_embs[i], payload={"doc_id": doc_ids[i]}) for i in range(len(doc_ids))]
        client.upsert(collection_name=collection_name, points=points)
    results = {}
    for idx, emb in enumerate(tqdm(query_embs, desc="Embedding-Xinference")):
        hits = client.search(collection_name=collection_name, query_vector=emb, limit=topk)
        retrieved = [hit.payload["doc_id"] for hit in hits]
        results[idx] = retrieved
    return results

# 5. 简单评估：召回率@10
def recall_at_k(results: Dict[int, List[str]], qrels: Dict[str, List[str]], query_ids: List[str], k=10):
    recalls = []
    for idx, retrieved in results.items():
        qid = query_ids[idx]
        relevant = set(qrels[qid])
        hit = len(set(retrieved[:k]) & relevant) > 0
        recalls.append(hit)
    return sum(recalls) / len(recalls)

def main():
    docs, doc_ids, queries, query_ids, qrels = load_nfcorpus()
    print("BM25测试...")
    bm25_results = bm25_recall(docs, queries, doc_ids)
    print("BM25 Recall@10:", recall_at_k(bm25_results, qrels, query_ids))
    # print("本地Embedding测试...")
    # emb_local_results = embedding_recall_local(docs, queries, doc_ids)
    # print("Embedding-Local Recall@10:", recall_at_k(emb_local_results, qrels, query_ids))
    # print("Xinference Embedding测试...")
    # emb_xinf_results = embedding_recall_xinference(docs, queries, doc_ids)
    # print("Embedding-Xinference Recall@10:", recall_at_k(emb_xinf_results, qrels, query_ids))

if __name__ == "__main__":
    main()
