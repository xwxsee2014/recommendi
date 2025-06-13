import tantivy
import re
import json
import os
import ir_datasets
import asyncio
import argparse
from tqdm import tqdm
import shutil  # Add this import for directory removal

DATASET = os.getenv("DATASET", "beir/quora/test")

def sanitize_query_for_tantivy(query):
    # escape special characters including apostrophes
    query = re.sub(r'([+\-!(){}\[\]^"~*?:\\<\'])', r' ', query)
    return query

def load_dataset():
    print(f"Loading dataset {DATASET}...")
    dataset = ir_datasets.load(DATASET)
    
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
    
    return docs, doc_ids, query_texts, query_ids, qrels_dict

def setup_tantivy_index(docs, doc_ids):
    print("Setting up Tantivy index...")
    file_out = f"data/tantivy_{DATASET.replace('/', '_')}/bm25.tantivy"
    
    if os.path.exists(file_out):
        # remove direcotry recursively
        shutil.rmtree(file_out)

    if not os.path.exists(file_out):
        os.makedirs(file_out, exist_ok=True)
    
    schema_builder = tantivy.SchemaBuilder()
    schema_builder.add_text_field("body", stored=True, tokenizer_name="en_stem")
    schema_builder.add_text_field("doc_id", stored=True)
    schema = schema_builder.build()
    
    print("Creating new index...")
    index = tantivy.Index(schema, path=file_out)
    
    # Index documents
    writer = index.writer()
    for i in tqdm(range(len(docs)), desc="Indexing documents"):
        writer.add_document(tantivy.Document(
            body=docs[i],
            doc_id=doc_ids[i]
        ))
    writer.commit()

def search_bm25(index, searcher, query, limit):
    query = index.parse_query(sanitize_query_for_tantivy(query), ['body'])
    hits = searcher.search(query, limit).hits
    docs = [
        searcher.doc(doc_address)
        for (score, doc_address) in hits
    ]
    return docs

async def search_bm25_async(index, searcher, query, limit):
    # Run the search in a thread pool to avoid blocking the event loop
    return await asyncio.get_event_loop().run_in_executor(
        None, 
        search_bm25, 
        index,
        searcher, 
        query, 
        limit
    )

def main(async_mode=False):
    # Load dataset
    docs, doc_ids, query_texts, query_ids, qrels_dict = load_dataset()
    
    # Setup Tantivy index
    setup_tantivy_index(docs, doc_ids)
    
    schema_builder = tantivy.SchemaBuilder()
    schema_builder.add_text_field("body", stored=True, tokenizer_name="en_stem")
    schema_builder.add_text_field("doc_id", stored=True)
    schema = schema_builder.build()
    index = tantivy.Index(schema, path=f"data/tantivy_{DATASET.replace('/', '_')}/bm25.tantivy/")

    searcher = index.searcher()
    print(f"Index contains {searcher.num_docs} documents.")
    
    if async_mode:
        asyncio.run(main_async(index, searcher, query_texts, query_ids, qrels_dict))
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
                
            # Search using BM25
            results = search_bm25(index, searcher, query_text, limit)
            retrieved_doc_ids = [hit["doc_id"][0] for hit in results]
            
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
        average_recall = sum(recalls) / len(recalls) if recalls else 0
        average_precision = sum(precisions) / len(precisions) if precisions else 0
        
        print(f"\nEvaluation results for {len(recalls)} queries:")
        print(f"Average Recall@{limit}: {average_recall:.4f}")
        print(f"Average Precision@{limit}: {average_precision:.4f}")

async def main_async(index, searcher, query_texts, query_ids, qrels_dict):
    print("Running in async mode...")
    
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
        tasks = [search_bm25_async(index, searcher, query_text, limit) for _, _, query_text in batch_queries]
        
        # Await all tasks concurrently
        results = await asyncio.gather(*tasks)
        
        # Process results
        for i, (idx, query_id, _) in enumerate(batch_queries):
            retrieved_doc_ids = [hit["doc_id"][0] for hit in results[i]]
            
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
    average_recall = sum(recalls) / len(recalls) if recalls else 0
    average_precision = sum(precisions) / len(precisions) if precisions else 0
    
    print(f"\nEvaluation results for {len(recalls)} queries:")
    print(f"Average Recall@{limit}: {average_recall:.4f}")
    print(f"Average Precision@{limit}: {average_precision:.4f}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Evaluate BM25 with Tantivy')
    parser.add_argument('--async', dest='async_mode', action='store_true', help='Run in async mode')
    args = parser.parse_args()
    
    main(async_mode=args.async_mode)
