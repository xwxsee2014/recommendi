import os
import shutil

input_base = "temp_input/smartcn"
output_plan_dir = "temp_output/smartcn/lesson_plan_test_data"
output_slide_dir = "temp_output/smartcn/lesson_slide_test_data"

os.makedirs(output_plan_dir, exist_ok=True)
os.makedirs(output_slide_dir, exist_ok=True)

for resource_id in os.listdir(input_base):
    resource_path = os.path.join(input_base, resource_id)
    if not os.path.isdir(resource_path):
        continue

    lesson_plan_path = os.path.join(resource_path, "lesson_plan.pdf")
    lesson_slide_path = os.path.join(resource_path, "lesson_slide.pdf")

    if os.path.isfile(lesson_plan_path):
        shutil.copyfile(lesson_plan_path, os.path.join(output_plan_dir, f"{resource_id}.pdf"))
    if os.path.isfile(lesson_slide_path):
        shutil.copyfile(lesson_slide_path, os.path.join(output_slide_dir, f"{resource_id}.pdf"))
