import os
import glob
import matplotlib.pyplot as plt
from transformers import AutoTokenizer
from process_pdf import ResourceProcessStatus
from smartcn_resource_download import TextbookTM
import json

def get_all_lesson_plan_md_files():
    # 1. 获取所有已处理的 course_bag_id
    output_dir = os.path.join(os.path.dirname(__file__), '../temp_output/smartcn/processed')
    course_bag_ids = ResourceProcessStatus.get_all_course_bag_ids()

    md_file_infos = []
    for course_bag_id in course_bag_ids:
        lesson_plan_dir = os.path.join(output_dir, course_bag_id, "lesson_plan")
        if not os.path.exists(lesson_plan_dir):
            continue
        md_files = glob.glob(os.path.join(lesson_plan_dir, "*.md"))
        for md_file in md_files:
            content_list_path = md_file.replace('.md', '_content_list.json')
            if os.path.exists(content_list_path):
                md_file_infos.append((md_file, content_list_path))
    print(f"被统计的文件数: {len(md_file_infos)}")
    return md_file_infos


def get_all_textbook_md_files():
    # 1. 获取所有已处理的 course_bag_id
    output_dir = os.path.join(os.path.dirname(__file__), '../temp_output/smartcn/tm_processed')
    textbook_ids = TextbookTM.get_all_prcessed_ids()

    md_file_infos = []
    for textbook_id in textbook_ids:
        textbook_dir = os.path.join(output_dir, textbook_id)
        if not os.path.exists(textbook_dir):
            continue
        md_files = glob.glob(os.path.join(textbook_dir, "*.md"))
        for md_file in md_files:
            content_list_path = md_file.replace('.md', '_content_list.json')
            if os.path.exists(content_list_path):
                md_file_infos.append((md_file, content_list_path))
    print(f"被统计的文件数: {len(md_file_infos)}")
    return md_file_infos


def count_words_in_md_files(md_file_infos):
    word_counts = []
    token_counts = []
    page_counts = []
    per_page_word_counts = []
    per_page_token_counts = []
    tokenizer = AutoTokenizer.from_pretrained("BAAI/bge-m3")

    for md_path, content_list_path in md_file_infos:
        with open(md_path, "r", encoding="utf-8") as f:
            text = f.read()
        word_count = len(text)
        tokens = tokenizer.encode(text, add_special_tokens=True)
        token_count = len(tokens)
        word_counts.append(word_count)
        token_counts.append(token_count)

        # 统计页数
        with open(content_list_path, "r", encoding="utf-8") as f:
            content_list = json.load(f)
        if not content_list:
            continue
        last_page_idx = content_list[-1].get("page_idx", None)
        if last_page_idx is None:
            continue
        page_num = last_page_idx + 1
        page_counts.append(page_num)
        per_page_word_counts.append(word_count / page_num)
        per_page_token_counts.append(token_count / page_num)

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

    # 画页数箱线图
    if page_counts:
        plt.figure()
        plt.boxplot(page_counts)
        plt.title('Page Count Boxplot')
        plt.ylabel('Page Count')
        plt.show()
        print(f"平均页数: {sum(page_counts) / len(page_counts):.2f}")

    # 每页平均字数和token数
    if per_page_word_counts:
        print(f"每页平均字数: {sum(per_page_word_counts) / len(per_page_word_counts):.2f}")
    if per_page_token_counts:
        print(f"每页平均token数: {sum(per_page_token_counts) / len(per_page_token_counts):.2f}")

if __name__ == "__main__":
    md_file_infos = get_all_lesson_plan_md_files()
    # md_file_infos = get_all_textbook_md_files()
    count_words_in_md_files(md_file_infos)
    print("统计完成")
