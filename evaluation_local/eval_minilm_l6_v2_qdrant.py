from qdrant_client import QdrantClient, models
from tqdm import tqdm
import ir_datasets
import os
import hashlib
import uuid
import asyncio
from qdrant_client.async_qdrant_client import AsyncQdrantClient
from sentence_transformers import SentenceTransformer

DATASET = "beir/quora/test"
QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY", None)
COLLECTION_NAME = DATASET.replace("/", "_") + "_minilm_l6_v2"

# Create a global model instance to avoid recreating it for each query
model = SentenceTransformer("all-MiniLM-L6-v2")

# Load nfcorpus data using ir_datasets
def load_nfcorpus():
    dataset = ir_datasets.load(DATASET)
    # corpus: dict[str, dict], queries: dict[str, str], qrels: dict[str, dict[str, int]]
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
    # qrels_dict 中最小长度
    min_relevant_docs = min(len(dids) for dids in qrels_dict.values()) if qrels_dict else 0
    # qrels_dict 中最大长度
    max_relevant_docs = max(len(dids) for dids in qrels_dict.values()) if qrels_dict else 0
    # qrels_dict 中平均长度
    avg_relevant_docs = sum(len(dids) for dids in qrels_dict.values()) / len(qrels_dict) if qrels_dict else 0
    # qrels_dict 中长度中位数
    median_relevant_docs = sorted(len(dids) for dids in qrels_dict.values())[len(qrels_dict) // 2] if qrels_dict else 0
    print(f"Median relevant documents per query: {median_relevant_docs}")
    print(f"Average relevant documents per query: {avg_relevant_docs:.2f}")
    print(f"Maximum relevant documents per query: {max_relevant_docs}")
    print(f"Minimum relevant documents per query: {min_relevant_docs}")

    return docs, doc_ids, query_texts, query_ids, qrels_dict

def index_docs(docs, doc_ids, reindex=False):
    print("Indexing documents with SentenceTransformer...")
    client = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY, prefer_grpc=True)
    
    # Delete collection if exists and reindex is True
    if client.collection_exists(COLLECTION_NAME):
        if reindex:
            client.delete_collection(COLLECTION_NAME)
        else:
            # If not reindexing, just return the client
            return client
    
    # Create collection with dense vector config
    client.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=models.VectorParams(
            size=384,
            distance=models.Distance.COSINE
        )
    )
    
    # Index documents in batches
    batch_size = 64
    for i in tqdm(range(0, len(docs), batch_size)):
        batch_docs = docs[i:i+batch_size]
        batch_ids = doc_ids[i:i+batch_size]
        
        # Get embeddings
        embeddings = model.encode(batch_docs, convert_to_numpy=True)
        
        # Create points
        points = []
        for j, embedding in enumerate(embeddings):
            doc_id = batch_ids[j]
            # If doc_id is purely numeric, use it directly; otherwise, use UUID
            if isinstance(doc_id, int):
                point_id = doc_id
            else:
                # Convert doc_id to UUID using MD5
                doc_id_str = str(doc_id)
                md5_hash = hashlib.md5(doc_id_str.encode()).hexdigest()
                point_id = str(uuid.UUID(md5_hash))
            
            points.append(models.PointStruct(
                id=point_id,
                vector=embeddings[j].tolist(),
                payload={"doc_id": doc_id, "text": batch_docs[j]}
            ))
        
        # Upload points
        client.upsert(
            collection_name=COLLECTION_NAME,
            points=points,
            wait=True
        )
    
    # print(f"Indexed {len(docs)} documents into collection '{COLLECTION_NAME}'")
    return client

def search_sparse(client, query, limit=10):
    query_vector = model.encode(query, convert_to_numpy=True)
    result = client.search(
        collection_name=COLLECTION_NAME,
        query_vector=query_vector,
        limit=limit,
        with_payload=True
    )
    return result

async def search_sparse_async(client, query, limit=10):
    # Run the CPU-bound embedding operation in a thread pool to avoid blocking the event loop
    query_vector = await asyncio.get_event_loop().run_in_executor(
        None, 
        lambda: model.encode(query, convert_to_numpy=True)
    )
    
    result = await client.query_points(
        collection_name=COLLECTION_NAME,
        query=query_vector,
        using="cosine",
        with_payload=True,
        limit=limit
    )
    return result

def main(async_mode=False, reindex=False):
    # Load data
    docs, doc_ids, query_texts, query_ids, qrels_dict = load_nfcorpus()

    # Index documents
    client = index_docs(docs, doc_ids, reindex=reindex)
    
    if async_mode:
        client.close()  # Close sync client if running async
        asyncio.run(main_async(docs, doc_ids, query_texts, query_ids, qrels_dict, reindex))
    else:
        # Evaluation
        limit = 10
        number_of_queries = min(len(query_texts), 100_000)
        
        recalls = []
        precisions = []
        
        for idx in tqdm(range(number_of_queries), desc="Evaluating queries"):
            query_id = query_ids[idx]
            query_text = query_texts[idx]
            
            # Skip queries without relevant documents
            if query_id not in qrels_dict or len(qrels_dict[query_id]) == 0:
                continue
                
            # Search using SentenceTransformer
            results = search_sparse(client, query_text, limit)
            retrieved_doc_ids = [hit.payload["doc_id"] for hit in results]
            
            # Calculate metrics
            relevant_doc_ids = qrels_dict[query_id]
            print(f"Query ID: {query_id}, Retrieved: {len(retrieved_doc_ids)}, Relevant: {len(relevant_doc_ids)}")
            relevant_retrieved = set(retrieved_doc_ids) & set(relevant_doc_ids)
            
            # Recall@10: proportion of relevant docs retrieved in top-10
            recall = len(relevant_retrieved) / len(relevant_doc_ids)
            recalls.append(recall)
            
            # Precision@10: proportion of retrieved docs that are relevant
            precision = len(relevant_retrieved) / limit
            precisions.append(precision)
            
            print(f"Query {idx+1}/{number_of_queries}: {query_id}, Recall@10: {recall:.3f}, Precision@10: {precision:.3f}")
    
        # Report overall results
        average_recall = sum(recalls) / len(recalls)
        average_precision = sum(precisions) / len(precisions)
        
        print(f"\nEvaluation results for {len(recalls)} queries:")
        print(f"Average Recall@{limit}: {average_recall:.4f}")
        print(f"Average Precision@{limit}: {average_precision:.4f}")

        client.close()  # Close the client after evaluation

async def main_async(docs, doc_ids, query_texts, query_ids, qrels_dict, reindex=False):
    print("Running in async mode...")
    # Create async client
    client = AsyncQdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY, prefer_grpc=True)
    # Note: Async indexing is not implemented; assumes sync indexing already done.
    # Evaluation
    limit = 10
    number_of_queries = min(len(query_texts), 100_000)
    
    recalls = []
    precisions = []
    
    # Process queries in batches for better async performance
    batch_size = 20  # Increased batch size for better concurrency
    for batch_start in tqdm(range(0, number_of_queries, batch_size), desc="Processing query batches"):
        batch_end = min(batch_start + batch_size, number_of_queries)
        batch_queries = []
        
        # Prepare the batch without awaiting
        for idx in range(batch_start, batch_end):
            query_id = query_ids[idx]
            query_text = query_texts[idx]
            
            # Skip queries without relevant documents
            if query_id not in qrels_dict or len(qrels_dict[query_id]) == 0:
                continue
                
            batch_queries.append((idx, query_id, query_text))
        
        # Create all tasks at once
        tasks = [search_sparse_async(client, query_text, limit) for _, _, query_text in batch_queries]
        
        # Await all tasks concurrently
        results = await asyncio.gather(*tasks)
        
        # Process results
        for i, (idx, query_id, _) in enumerate(batch_queries):
            retrieved_doc_ids = [hit.payload["doc_id"] for hit in results[i]]
            
            # Calculate metrics
            relevant_doc_ids = qrels_dict[query_id]
            relevant_retrieved = set(retrieved_doc_ids) & set(relevant_doc_ids)
            
            # Recall@10: proportion of relevant docs retrieved in top-10
            recall = len(relevant_retrieved) / len(relevant_doc_ids)
            recalls.append(recall)
            
            # Precision@10: proportion of retrieved docs that are relevant
            precision = len(relevant_retrieved) / limit
            precisions.append(precision)
            
            print(f"Query {idx+1}/{number_of_queries}: {query_id}, Recall@10: {recall:.3f}, Precision@10: {precision:.3f}")
    
    # Report overall results
    average_recall = sum(recalls) / len(recalls)
    average_precision = sum(precisions) / len(precisions)
    
    print(f"\nEvaluation results for {len(recalls)} queries:")
    print(f"Average Recall@{limit}: {average_recall:.4f}")
    print(f"Average Precision@{limit}: {average_precision:.4f}")
    
    await client.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Evaluate BM25 with Qdrant')
    parser.add_argument('--async', dest='async_mode', action='store_true', help='Run in async mode')
    parser.add_argument('--reindex', action='store_true', help='Reindex documents (delete and recreate collection)')
    args = parser.parse_args()
    
    main(async_mode=args.async_mode, reindex=args.reindex)
