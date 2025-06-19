import json
import random
import csv
from xinference.client import Client

import numpy as np
import re

def get_sampled_items(jsonl_path, sample_ratio=0.01):
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        data = [json.loads(line) for line in f]
    filtered = [item for item in data if 'paragraphs' in item and isinstance(item['paragraphs'], list) and len(item['paragraphs']) > 2]
    sample_size = max(1, int(len(filtered) * sample_ratio))
    sampled = random.sample(filtered, sample_size)
    return sampled

def sample_and_calc_similarity(sampled, csv_output_dir, model_uid, resource_category):
    # 初始化 embedding 模型
    client = Client("http://localhost:9998", api_key="sk-72tkvudyGLPMi")
    model = client.get_model(model_uid)

    results = []
    for item in sampled:
        doc_id = item.get('doc_id', '')
        print(f"Processing document ID: {doc_id}")
        paragraphs = item['paragraphs']
        # 计算每个相邻段落的 embedding 相似度
        for i in range(len(paragraphs) - 1):
            para1 = paragraphs[i]
            para2 = paragraphs[i+1]
            # 去掉 'paragraph_{number}:' 前缀并 strip
            para1 = re.sub(r'^paragraph_\d+:\s*', '', para1).strip()
            para2 = re.sub(r'^paragraph_\d+:\s*', '', para2).strip()
            emb1 = model.create_embedding(para1).get('data', [])[0].get('embedding', [])
            emb2 = model.create_embedding(para2).get('data', [])[0].get('embedding', [])
            # 计算余弦相似度
            v1 = np.array(emb1)
            v2 = np.array(emb2)
            score = float(np.dot(v1, v2) / (np.linalg.norm(v1) * np.linalg.norm(v2)))
            results.append({
                'doc_id': doc_id,
                'para1': para1,
                'para2': para2,
                'score': score
            })

    # 保存到 csv
    csv_output_path = f"{csv_output_dir}/{resource_category}_paragraph_similarity_{model_uid}.csv"
    with open(csv_output_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['doc_id', 'para1', 'para2', 'score'])
        writer.writeheader()
        for row in results:
            writer.writerow(row)

def sample_and_rerank_similarity(sampled, csv_output_dir, model_uid, resource_category):
    # 初始化 embedding 模型
    client = Client("http://localhost:9998", api_key="sk-72tkvudyGLPMi")
    model = client.get_model(model_uid)

    results = []
    for item in sampled:
        doc_id = item.get('doc_id', '')
        print(f"Processing document ID: {doc_id}")
        paragraphs = item['paragraphs']
        # 计算每个相邻段落的 rerank 相似度
        for i in range(len(paragraphs) - 1):
            para1 = paragraphs[i]
            para2 = paragraphs[i+1]
            # 去掉 'paragraph_{number}:' 前缀并 strip
            para1_clean = re.sub(r'^paragraph_\d+:\s*', '', para1).strip()
            para2_clean = re.sub(r'^paragraph_\d+:\s*', '', para2).strip()
            # 使用 rerank 方法
            query = para2_clean
            corpus = [para1_clean]
            rerank_result = model.rerank(corpus, query)
            # 假设 rerank 返回的是 [{'corpus_index': 0, 'score': ...}]
            score = rerank_result['results'][0]['relevance_score']
            results.append({
                'doc_id': doc_id,
                'para1': para1_clean,
                'para2': para2_clean,
                'score': score
            })

    # 保存到 csv
    csv_output_path = f"{csv_output_dir}/{resource_category}_paragraph_similarity_{model_uid}.csv"
    with open(csv_output_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['doc_id', 'para1', 'para2', 'score'])
        writer.writeheader()
        for row in results:
            writer.writerow(row)

if __name__ == "__main__":
    resource_category = "lesson_plan"
    jsonl_path = f'temp_output/smartcn/ir_datasets_splitted/{resource_category}/documents_merged.jsonl'  # 替换为你的 jsonl 文件路径
    csv_output_dir = 'temp_output/smartcn/similarity'
    embdding_model_uid = 'bge-m3'  # 替换为你的模型 UID
    rerank_model_uid = 'bge-reranker-v2-m3'  # 替换为你的 rerank 模型 UID

    sampled = get_sampled_items(jsonl_path, sample_ratio=0.01)
    sample_and_calc_similarity(sampled, csv_output_dir, embdding_model_uid, resource_category)
    sample_and_rerank_similarity(sampled, csv_output_dir, rerank_model_uid, resource_category)
