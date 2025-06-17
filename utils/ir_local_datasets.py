import os
import json
import yaml

class Document:
    def __init__(self, doc_id, text):
        self.doc_id = doc_id
        self.text = text

class Query:
    def __init__(self, query_id, text):
        self.query_id = query_id
        self.text = text

class Qrel:
    def __init__(self, query_id, doc_id, relevance):
        self.query_id = query_id
        self.doc_id = doc_id
        self.relevance = relevance

class LocalDataset:
    def __init__(self, dataset_path):
        self.dataset_path = dataset_path
        self.metadata = self._load_metadata()
        self.name = self.metadata.get("dataset", os.path.basename(dataset_path))
        self.docs = self._load_docs()
        self.queries = self._load_queries()
        self.qrels = self._load_qrels()

    def _load_metadata(self):
        metadata_path = os.path.join(self.dataset_path, "metadata.yaml")
        with open(metadata_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _load_docs(self):
        docs_info = self.metadata.get("files").get("documents", {})
        docs_path = os.path.join(self.dataset_path, docs_info.get("path", "docs.jsonl"))
        doc_id_field = docs_info.get("id_field", "doc_id")
        text_field = docs_info.get("text_field", "text")
        docs = []
        if os.path.exists(docs_path):
            with open(docs_path, "r", encoding="utf-8") as f:
                for line in f:
                    obj = json.loads(line)
                    docs.append(Document(obj[doc_id_field], obj[text_field]))
        return docs

    def _load_queries(self):
        queries_info = self.metadata.get("files").get("queries", {})
        queries_path = os.path.join(self.dataset_path, queries_info.get("path", "queries.jsonl"))
        query_id_field = queries_info.get("id_field", "query_id")
        text_field = queries_info.get("text_field", "text")
        queries = []
        if os.path.exists(queries_path):
            with open(queries_path, "r", encoding="utf-8") as f:
                for line in f:
                    obj = json.loads(line)
                    queries.append(Query(obj[query_id_field], obj[text_field]))
        return queries

    def _load_qrels(self):
        qrels_info = self.metadata.get("files").get("qrels", {})
        qrels_path = os.path.join(self.dataset_path, qrels_info.get("path", "qrels.jsonl"))
        query_id_field = qrels_info.get("query_id_field", "query_id")
        doc_id_field = qrels_info.get("doc_id_field", "doc_id")
        relevance_field = qrels_info.get("relevance_field", "relevance")
        qrels = []
        if os.path.exists(qrels_path):
            with open(qrels_path, "r", encoding="utf-8") as f:
                for line in f:
                    obj = json.loads(line)
                    qrels.append(Qrel(obj[query_id_field], obj[doc_id_field], obj[relevance_field]))
        return qrels

    def docs_iter(self):
        for doc in self.docs:
            yield doc

    def queries_iter(self):
        for query in self.queries:
            yield query

    def qrels_iter(self):
        for qrel in self.qrels:
            yield qrel

def load(dataset_path):
    """
    Load a local IR dataset from the given directory.
    """
    return LocalDataset(dataset_path)
