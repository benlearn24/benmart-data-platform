import zipfile
import os

project_root = os.path.dirname(os.path.abspath(__file__))
zip_path = os.path.join(project_root, "glue_package.zip")

with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
    for folder in ['src']:
        for root, dirs, files in os.walk(os.path.join(project_root, folder)):
            for file in files:
                if file.endswith('.py'):
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, project_root)
                    zf.write(file_path, arcname)

print(f"Package created: {zip_path}")

for name in zipfile.ZipFile(zip_path).namelist():
    print(f"  {name}")