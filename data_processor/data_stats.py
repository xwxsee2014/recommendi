import os
import glob
import matplotlib.pyplot as plt
from transformers import AutoTokenizer
from process_pdf import ResourceProcessStatus
from smartcn_resource_download import TextbookTM

def get_all_lesson_plan_md_files():
    # 1. 获取所有已处理的 course_bag_id
    output_dir = os.path.join(os.path.dirname(__file__), '../temp_output/smartcn/processed')
    course_bag_ids = ResourceProcessStatus.get_all_course_bag_ids()

    md_file_paths = []
    for course_bag_id in course_bag_ids:
        lesson_plan_dir = os.path.join(output_dir, course_bag_id, "lesson_plan")
        if not os.path.exists(lesson_plan_dir):
            continue
        md_files = glob.glob(os.path.join(lesson_plan_dir, "*.md"))
        md_file_paths.extend(md_files)

    print(f"被统计的文件数: {len(md_file_paths)}")
    return md_file_paths


def get_all_textbook_md_files():
    # 1. 获取所有已处理的 course_bag_id
    output_dir = os.path.join(os.path.dirname(__file__), '../temp_output/smartcn/tm_processed')
    textbook_ids = TextbookTM.get_all_prcessed_ids()

    md_file_paths = []
    for textbook_id in textbook_ids:
        textbook_dir = os.path.join(output_dir, textbook_id)
        if not os.path.exists(textbook_dir):
            continue
        md_files = glob.glob(os.path.join(textbook_dir, "*.md"))
        md_file_paths.extend(md_files)

    print(f"被统计的文件数: {len(md_file_paths)}")
    return md_file_paths


def count_words_in_md_files(md_file_paths):
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

if __name__ == "__main__":
    # md_file_paths = get_all_lesson_plan_md_files()
    md_file_paths = get_all_textbook_md_files()
    count_words_in_md_files(md_file_paths)
    print("统计完成")
