import os
import shutil
import hashlib
from fastapi import UploadFile

STORAGE_ROOT = "d:/WorkShop/Practice/PythonWorkSpace/ObjectStorage/data"

os.makedirs(STORAGE_ROOT, exist_ok=True)

def get_file_path(storage_path: str) -> str:
    return os.path.join(STORAGE_ROOT, storage_path)

def save_file(file_obj: UploadFile, bucket: str, key: str, version_id: str) -> str:
    """
    Saves the file to disk and returns the relative storage path.
    Structure: data/{bucket}/{key_hash}/{version_id}
    Using key_hash to avoid filesystem issues with weird keys.
    """
    # Create a safe directory structure
    key_hash = hashlib.md5(key.encode()).hexdigest()
    directory = os.path.join(STORAGE_ROOT, bucket, key_hash)
    os.makedirs(directory, exist_ok=True)
    
    filename = version_id
    file_path = os.path.join(directory, filename)
    
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file_obj.file, buffer)
        
    # Return path relative to STORAGE_ROOT
    return os.path.join(bucket, key_hash, filename)

def get_file_content(storage_path: str):
    full_path = get_file_path(storage_path)
    if not os.path.exists(full_path):
        return None
    return open(full_path, "rb")

def delete_physical_file(storage_path: str):
    full_path = get_file_path(storage_path)
    if os.path.exists(full_path):
        os.remove(full_path)

def calculate_etag(file_obj: UploadFile) -> str:
    # Reset file pointer just in case
    file_obj.file.seek(0)
    hash_md5 = hashlib.md5()
    for chunk in iter(lambda: file_obj.file.read(4096), b""):
        hash_md5.update(chunk)
    file_obj.file.seek(0) # Reset after reading
    return f'"{hash_md5.hexdigest()}"'
