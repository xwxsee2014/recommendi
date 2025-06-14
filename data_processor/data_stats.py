import os
import glob
import matplotlib.pyplot as plt
from transformers import AutoTokenizer
from data_processor.process_pdf import ResourceProcessStatus

# 1. 获取所有已处理的 course_bag_id
output_dir = os.path.join(os.path.dirname(__file__), '../temp_output/smartcn/processed')
course_bag_ids = [
    name for name in os.listdir(output_dir)
    if os.path.isdir(os.path.join(output_dir, name))
]

md_file_paths = []
for course_bag_id in course_bag_ids:
    lesson_plan_dir = os.path.join(output_dir, course_bag_id, "lesson_plan")
    if not os.path.exists(lesson_plan_dir):
        continue
    md_files = glob.glob(os.path.join(lesson_plan_dir, "*.md"))
    md_file_paths.extend(md_files)

word_counts = []
token_counts = []
tokenizer = AutoTokenizer.from_pretrained("BAAI/bge-m3")

for md_path in md_file_paths:
    with open(md_path, "r", encoding="utf-8") as f:
        text = f.read()
    word_counts.append(len(text))
    tokens = tokenizer.encode(text, add_special_tokens=True)
    token_counts.append(len(tokens))

# 画字数箱线图
plt.figure()
plt.boxplot(word_counts)
plt.title('Word Count Boxplot')
plt.ylabel('Word Count')
plt.show()

print(f"平均字数: {sum(word_counts) / len(word_counts):.2f}")

# 画token数箱线图
plt.figure()
plt.boxplot(token_counts)
plt.title('Token Count Boxplot')
plt.ylabel('Token Count')
plt.show()

print(f"平均token数: {sum(token_counts) / len(token_counts):.2f}")
