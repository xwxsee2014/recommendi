from elasticsearch import Elasticsearch, helpers
import re
import json
import os
import utils.ir_local_datasets as ir_datasets
import argparse
from tqdm import tqdm
import shutil

DATASET = "temp_output/smartcn/ir_datasets/lesson_plan"
ES_INDEX_PREFIX = "bm25_es_"

def sanitize_query_for_es(query):
    # Elasticsearch 会自动处理大部分特殊字符，但可适当清理
    query = re.sub(r'([+\-!(){}\[\]^"~*?:\\<\'])', r' ', query)
    return query

def load_dataset():
    print(f"Loading dataset {DATASET}...")
    dataset = ir_datasets.load(DATASET)
    dataset_name = dataset.name
    
    # Load documents, queries, and relevance judgments
    corpus = {doc.doc_id: {"text": doc.text} for doc in dataset.docs_iter()}
    queries = {q.query_id: q.text for q in dataset.queries_iter()}
    qrels = {}
    for qrel in dataset.qrels_iter():
        if qrel.query_id not in qrels:
            qrels[qrel.query_id] = {}
        qrels[qrel.query_id][qrel.doc_id] = qrel.relevance
    
    # Convert to list format
    docs = [corpus[doc_id]["text"] for doc_id in corpus]
    doc_ids = list(corpus.keys())
    query_texts = [queries[qid] for qid in queries]
    query_ids = list(queries.keys())
    
    # qrels_dict: {query_id: [doc_id, ...]} only keep docs with relevance > 0
    qrels_dict = {}
    for qid, doc_dict in qrels.items():
        qrels_dict[qid] = [did for did, rel in doc_dict.items() if rel > 0]
    
    print(f"{len(docs)} documents loaded.")
    print(f"{len(query_texts)} queries loaded.")
    print(f"{len(qrels_dict)} queries with relevant documents.")
    
    # Calculate statistics
    if qrels_dict:
        min_relevant_docs = min(len(dids) for dids in qrels_dict.values())
        max_relevant_docs = max(len(dids) for dids in qrels_dict.values())
        avg_relevant_docs = sum(len(dids) for dids in qrels_dict.values()) / len(qrels_dict)
        median_relevant_docs = sorted(len(dids) for dids in qrels_dict.values())[len(qrels_dict) // 2]
        print(f"Median relevant documents per query: {median_relevant_docs}")
        print(f"Average relevant documents per query: {avg_relevant_docs:.2f}")
        print(f"Maximum relevant documents per query: {max_relevant_docs}")
        print(f"Minimum relevant documents per query: {min_relevant_docs}")
    
    return dataset_name, docs, doc_ids, query_texts, query_ids, qrels_dict

def setup_es_index(dataset_name, docs, doc_ids):
    print("Setting up Elasticsearch index...")
    es = Elasticsearch(
        hosts=["http://localhost:9200"],
        http_auth=("elastic", "changeme"),
        scheme="http",
        port=9200,
        verify_certs=False
    )
    index_name = ES_INDEX_PREFIX + dataset_name.replace("/", "_")
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
    # 创建 mapping，使用 ik_smart 分词器
    mapping = {
        "settings": {
            "analysis": {
                "analyzer": {
                    "default": {
                        "type": "ik_smart"
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "body": {
                    "type": "text",
                    "analyzer": "ik_smart",
                    "search_analyzer": "ik_smart"
                },
                "doc_id": {
                    "type": "keyword"
                }
            }
        }
    }
    es.indices.create(index=index_name, body=mapping)
    # 批量写入文档
    actions = [
        {
            "_index": index_name,
            "_id": doc_ids[i],
            "_source": {
                "body": docs[i],
                "doc_id": doc_ids[i]
            }
        }
        for i in range(len(docs))
    ]
    helpers.bulk(es, actions)
    print(f"Indexed {len(docs)} documents to {index_name}.")
    # flush index to ensure it's ready for search
    es.indices.refresh(index=index_name)
    return es, index_name

def search_bm25(es, index_name, query, limit):
    query = sanitize_query_for_es(query)
    body = {
        "query": {
            "match": {
                "body": {
                    "query": query,
                    "analyzer": "ik_smart"
                }
            }
        }
    }
    res = es.search(index=index_name, body=body, size=limit)
    hits = res["hits"]["hits"]
    return hits

def main(async_mode=False):
    # Load dataset
    dataset_name, docs, doc_ids, query_texts, query_ids, qrels_dict = load_dataset()
    # Setup Elasticsearch index
    es, index_name = setup_es_index(dataset_name, docs, doc_ids)
    print(f"Index contains {es.count(index=index_name)['count']} documents.")
    limit = 10
    number_of_queries = min(len(query_texts), 100_000)
    recalls = []
    precisions = []
    for idx in tqdm(range(number_of_queries), desc="Evaluating queries"):
        query_id = query_ids[idx]
        query_text = query_texts[idx]
        if query_id not in qrels_dict or len(qrels_dict[query_id]) == 0:
            continue
        results = search_bm25(es, index_name, query_text, limit)
        retrieved_doc_ids = [hit["_source"]["doc_id"] for hit in results]
        relevant_doc_ids = qrels_dict[query_id]
        relevant_retrieved = set(retrieved_doc_ids) & set(relevant_doc_ids)
        recall = len(relevant_retrieved) / len(relevant_doc_ids)
        recalls.append(recall)
        precision = len(relevant_retrieved) / limit
        precisions.append(precision)
        print(f"Query {idx+1}/{number_of_queries}: {query_id}, Recall@10: {recall:.3f}, Precision@10: {precision:.3f}")
    average_recall = sum(recalls) / len(recalls) if recalls else 0
    average_precision = sum(precisions) / len(precisions) if precisions else 0
    print(f"\nEvaluation results for {len(recalls)} queries:")
    print(f"Average Recall@{limit}: {average_recall:.4f}")
    print(f"Average Precision@{limit}: {average_precision:.4f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Evaluate BM25 with Tantivy')
    args = parser.parse_args()
    main()
