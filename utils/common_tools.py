import sys
import os
import io
import json
import base64
import hashlib
import subprocess
import numpy as np
from io import BytesIO
from PIL import Image
from skimage.color import rgb2lab, rgb2gray, gray2rgb, rgba2rgb
from skimage.metrics import mean_squared_error
from skimage.transform import resize

def check_first_and_last_line_contain_triple_backticks(s):  
    # 去除字符串两端的空白字符（包括换行符）  
    s = s.strip()  
      
    # 如果字符串为空，直接返回False  
    if not s:  
        return False  
      
    # 检查字符串是否以换行符开头和结尾  
    # 如果不是，那么整个字符串就是一行，直接检查是否包含'```'  
    if not s.startswith('\n') or not s.endswith('\n'):  
        return s.startswith('```') and s.endswith('```')  
      
    # 如果字符串以换行符开头和结尾，那么去掉它们，并分割成行  
    lines = s.strip('\n').split('\n')  
      
    # 检查第一行和最后一行是否都包含'```'，并返回去除第一行和最后一行的列表  
    return lines[0].startswith('```') and lines[-1].endswith('```'), lines[1:-1]

def read_text_from_file():
    text = ""
    path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "sample_data.txt")
    if os.path.exists(path):
        with open(path, "r") as file:
            text = file.read()
    else:
        FileNotFoundError(f"File not found: {path}")
    return text

def read_json_to_dict(file_path):
    try:
        with open(file_path, 'r', encoding='UTF-8') as file:
            return json.load(file)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None

def process_directory(dir_path, parent_dict):
    for item in os.listdir(dir_path):
        item_path = os.path.join(dir_path, item)
        if os.path.isdir(item_path):
            # If the item is a directory, recurse with a new or existing sub-dictionary
            process_directory(item_path, parent_dict.setdefault(item, {}))
        elif item.endswith('.json'):
            # If the item is a JSON file, read it and store the data
            parent_dict[item[:-5]] = read_json_to_dict(item_path)  # Removes '.json' from the file name for the key

def walk_and_load_jsons(root_dir):
    dict_all = {}
    process_directory(root_dir, dict_all)
    return dict_all

def path_validator(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path

def retry(max_retries=3):
    def decorator(func):
        def wrapper(*args, **kwargs):
            last_exception = None
            for _ in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
            raise last_exception
        return wrapper
    return decorator

def save_base64_image(base64_string, output_folder, filename):
    # Ensure the output folder exists
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    # Decode the base64 string
    image_data = base64.b64decode(base64_string)
    
    # Create the full path for the output file
    output_path = os.path.join(output_folder, filename)
    
    # Write the image data to the file
    with open(output_path, 'wb') as output_file:
        output_file.write(image_data)

def calculate_md5(file_path):
    md5_hash = hashlib.md5()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            md5_hash.update(byte_block)
    return md5_hash.hexdigest()

def calculate_string_md5(string):
    md5_hash = hashlib.md5()
    md5_hash.update(string.encode('utf-8'))
    return md5_hash.hexdigest()

def pil_image_to_base64(image):
    buffered = io.BytesIO()
    image.save(buffered, format="PNG")
    img_bytes = buffered.getvalue()
    img_base64 = base64.b64encode(img_bytes).decode('utf-8')
    return img_base64

def decode_base64_to_image(base64_str):
    image_data = base64.b64decode(base64_str)
    image = Image.open(BytesIO(image_data))
    return image

def encode_image(image_path):
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode("utf-8")
    
def decode_image(image_base64, image_path):
    with open(image_path, "wb") as image_file:
        image_file.write(base64.b64decode(image_base64))

def base64_image_to_file(base64_image, output_file_path):
    with open(output_file_path, 'wb') as output_file:
        output_file.write(base64.b64decode(base64_image))
    
def transform_to_pdf(source_file_path, pdf_dir_path):
    if not os.path.exists(pdf_dir_path):
        os.makedirs(pdf_dir_path)
    command = [
        'soffice', '--headless', '--invisible', '--convert-to', 'pdf:writer_pdf_Export',
        os.path.abspath(source_file_path), '--outdir', os.path.abspath(pdf_dir_path)
    ]
    subprocess.run(command, check=True)

    source_file_ext = os.path.splitext(source_file_path)[1]
    file_basename_without_ext = os.path.basename(source_file_path).replace(source_file_ext, '')
    pdf_path = os.path.join(pdf_dir_path, f"{file_basename_without_ext}.pdf")
    pdf_md5 = calculate_md5(pdf_path)
    os.rename(pdf_path, os.path.join(pdf_dir_path, f"{file_basename_without_ext}_{pdf_md5}.pdf"))
    renamed_pdf_path = os.path.join(pdf_dir_path, f"{file_basename_without_ext}_{pdf_md5}.pdf")
    return renamed_pdf_path

def transform_to_pdf_wps(source_file_path, pdf_dir_path):
    file_basename_without_ext = os.path.basename(source_file_path).replace(os.path.splitext(source_file_path)[1], '')
    pdf_output_path = os.path.join(pdf_dir_path, f"{file_basename_without_ext}.pdf")
    if os.path.exists(pdf_output_path):
        return pdf_output_path
    try:
        # 初始化WPS的COM对象
        import comtypes.client
        wps = comtypes.client.CreateObject("KWPP.Application")  # WPS演示的COM对象
        # 打开PPTX文件
        deck = wps.Presentations.Open(os.path.abspath(source_file_path))
        # 保存为PDF
        deck.SaveAs(os.path.abspath(pdf_output_path), 32)  # 32是PDF的文件格式代码
        deck.Close()
    except Exception as e:
        raise ValueError(f"转换失败: {e}")
    finally:
        # 关闭WPS应用程序
        wps.Quit()
    return pdf_output_path

def get_fonts_by_language(font_key="en"):
    fonts_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "fonts")
    fonts = {
        "en": "arial.ttf",
        "zh": "simsun.ttc",
        "ar": os.path.join(fonts_dir, "trado.ttf"),
        "ar-bold": os.path.join(fonts_dir, "tradbdo.ttf"),
        "th": os.path.join(fonts_dir, "THSarabunNew.ttf"),
        "th-bold": os.path.join(fonts_dir, "THSarabunNew Bold.ttf"),
        "th-italic": os.path.join(fonts_dir, "THSarabunNew Italic.ttf"),
        "th-bold-italic": os.path.join(fonts_dir, "THSarabunNew BoldItalic.ttf"),
    }
    if font_key not in fonts:
        raise ValueError(f"Font key {font_key} is not supported yet!")
    return fonts.get(font_key, "arial.ttf")

def images_similar_value(image1_pil: Image.Image, image2_pil: Image.Image):
    if image2_pil is None:
        return sys.maxsize
    image1 = _read_image(image1_pil)
    image2 = _read_image(image2_pil)
    shape1 = image1.shape[:2]
    shape2 = image2.shape[:2]
    aspect_ratio1 = shape1[1] / shape1[0]
    aspect_ratio2 = shape2[1] / shape1[0]
    shape_proportional = abs(aspect_ratio1 - aspect_ratio2) < 0.2
    if not shape_proportional:
        return sys.maxsize

    if image1.shape[-1] == 3:
        image1 = rgb2gray(image1)
    if image2.shape[-1] == 3:
        image2 = rgb2gray(image2)

    if shape1[0] > shape2[0] or shape1[1] > shape2[1]:
        new_image1 = resize(image1, (shape2[0], shape2[1]), anti_aliasing=True)
        new_image2 = image2
    else:
        new_image1 = image1
        new_image2 = resize(image2, (shape1[0], shape1[1]), anti_aliasing=True)
    pixel_diff = mean_squared_error(new_image1, new_image2)
    return pixel_diff

def _read_image(image_pil: Image.Image):
    image_array = np.array(image_pil)
    if image_array.shape[-1] == 4:
        # Convert RGBA to RGB
        image_rgb = rgba2rgb(image_array)
    elif image_array.shape[-1] != 3:
        # Convert grayscale to RGB
        image_rgb = gray2rgb(image_array)
    else:
        image_rgb = image_array
    return image_rgb

def resize_and_crop_image(img1_path, img2_path):
    # 打开img1
    img1 = Image.open(img1_path)
    img2 = Image.open(img2_path)

    # 获取img2的宽度和高度
    img2_width, img2_height = img2.size

    # 调整img1的高度到img2的高度，并且宽度同比例调整
    new_height = img2_height
    new_width = int((new_height / img1.height) * img1.width)
    img1_resized = img1.resize((new_width, new_height), resample=Image.Resampling.LANCZOS)

    # 计算裁剪的起始和结束位置
    left = (img1_resized.width - img2_width) // 2
    right = left + img2_width

    # 裁剪图像
    img1_cropped = img1_resized.crop((left, 0, right, img1_resized.height))
    return img1_cropped

def resize_image_half_to_base64(input_path):
    try:
        image = Image.open(input_path)
        original_size = image.size
        new_size = (original_size[0] // 2, original_size[1] // 2)
        resized_image = image.resize(new_size, resample=Image.Resampling.LANCZOS)
        return pil_image_to_base64(resized_image)
    except Exception as e:
        raise e
    
def resize_image_half(image):
    try:
        original_size = image.size
        new_size = (original_size[0] // 2, original_size[1] // 2)
        resized_image = image.resize(new_size, resample=Image.Resampling.LANCZOS)
        return resized_image
    except Exception as e:
        raise e
    
def resize_base64_image_half_to_base64(base64_image):
    try:
        image = decode_base64_to_image(base64_image)
        original_size = image.size
        new_size = (original_size[0] // 2, original_size[1] // 2)
        resized_image = image.resize(new_size, resample=Image.Resampling.LANCZOS)
        return pil_image_to_base64(resized_image)
    except Exception as e:
        raise e
    
def resize_base64_image_by_input_crop_params(base64_image, crop_params):
    try:
        image = decode_base64_to_image(base64_image)
        width, height = image.size
        left = int(crop_params["left"] * width)
        upper = int(crop_params["top"] * height)
        right = width - int(crop_params["right"] * width)
        lower = height - int(crop_params["bottom"] * height)
        cropped_image = image.crop((left, upper, right, lower))
        return pil_image_to_base64(cropped_image)
    except Exception as e:
        raise e

def resize_and_crop_pil_image(img1, img2):
    # 获取img2的宽度和高度
    img2_width, img2_height = img2.size

    # 调整img1的高度到img2的高度，并且宽度同比例调整
    new_height = img2_height
    new_width = int((new_height / img1.height) * img1.width)
    img1_resized = img1.resize((new_width, new_height), resample=Image.Resampling.LANCZOS)

    # 计算裁剪的起始和结束位置
    left = (img1_resized.width - img2_width) // 2
    right = left + img2_width

    # 裁剪图像
    img1_cropped = img1_resized.crop((left, 0, right, img1_resized.height))
    return img1_cropped

def is_video_file(filename):
    video_extensions = {'.mp4', '.avi', '.mkv', '.mov', '.flv', '.wmv', '.webm'}
    ext = os.path.splitext(filename)[1].lower()
    return ext in video_extensions

def copy_images_with_folder_prefix(input_folder, output_folder, manual_prefix=None):
    """
    Copies all images from input_folder (including subdirectories) to output_folder
    with a prefix based on the directory structure.
    
    The new filename format is: {input_folder_name}_{subdir_name}_{original_filename}
    
    Args:
        input_folder (str): The source directory containing images
        output_folder (str): The destination directory where images will be copied
    """
    import shutil
    
    # Create output folder if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    # Get base name of input folder
    input_folder_base = os.path.basename(input_folder)
    
    # Define image file extensions
    image_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp'}
    
    # Walk through all directories and files in input_folder
    for root, _, files in os.walk(input_folder):
        for file in files:
            # Check if file is an image
            file_ext = os.path.splitext(file)[1].lower()
            if file_ext in image_extensions:
                # Get the relative path from input_folder to the file's directory
                rel_path = os.path.relpath(root, input_folder)
                
                # Create the prefix
                if rel_path == '.':  # file is directly in input_folder
                    prefix = input_folder_base
                else:
                    # Replace directory separators with underscores
                    subdir_part = rel_path.replace(os.path.sep, '_')
                    prefix = f"{input_folder_base}_{subdir_part}"
                
                # Create new filename
                new_filename = f"{prefix}_{file}"

                if manual_prefix:
                    new_filename = f"{manual_prefix}_{new_filename}"
                
                # Copy the file
                src_path = os.path.join(root, file)
                dst_path = os.path.join(output_folder, new_filename)
                
                # Copy the file preserving metadata
                shutil.copy2(src_path, dst_path)

def adjust_and_write_tikz_to_tex(tikz_code: str, output_tex_path: str):
    """
    调整tikz代码并写入tex文件，生成的tex可直接编译运行。
    参数:
        tikz_code (str): 原始tikz代码字符串
        output_tex_path (str): 输出tex文件路径
    """
    # 可在此处对tikz_code做进一步调整（如缩放、注释、替换等）
    # 这里直接写入
    with open(output_tex_path, "w", encoding="utf-8") as f:
        f.write(tikz_code)
