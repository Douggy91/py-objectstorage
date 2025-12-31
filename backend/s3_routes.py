import uuid
import os
import hashlib
from datetime import datetime
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Request, Response, Header, UploadFile, File
from sqlmodel import Session, select
from backend.database import get_session
from backend.models import Bucket, ObjectVersion, Owner
from backend.storage import save_file, get_file_content, calculate_etag, delete_physical_file, get_file_path, STORAGE_ROOT

router = APIRouter()

# --- Helpers ---
def generate_xml_response(content: str, status_code: int = 200):
    return Response(content=content, media_type="application/xml", status_code=status_code)

def get_iso_timestamp(dt: datetime):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

# --- Internal API for GUI ---
from pydantic import BaseModel
from backend.auth import VALID_CREDENTIALS, create_token, get_current_user

class LoginRequest(BaseModel):
    username: str
    password: str

@router.post("/api/login")
async def login(creds: LoginRequest):
    if creds.username in VALID_CREDENTIALS and VALID_CREDENTIALS[creds.username] == creds.password:
        token = create_token(creds.username)
        return {"token": token}
    raise HTTPException(status_code=401, detail="Invalid credentials")

# Returning JSON for easier frontend
@router.get("/api/ui/buckets")
async def ui_list_buckets(session: Session = Depends(get_session), user: str = Depends(get_current_user)):
    stmt = select(Bucket)
    buckets = session.exec(stmt).all()
    return [{"name": b.name, "creation_date": b.creation_date} for b in buckets]

@router.get("/api/ui/{bucket_name}/objects")
async def ui_list_objects(bucket_name: str, session: Session = Depends(get_session), user: str = Depends(get_current_user)):
    # Group by Key, show latest status
    # Or just list all unique keys?
    # Let's return list of keys with their current status
    stmt = select(ObjectVersion).where(ObjectVersion.bucket_name == bucket_name).order_by(ObjectVersion.key, ObjectVersion.last_modified.desc())
    all_versions = session.exec(stmt).all()
    
    # Process into structured data
    # { "key": [ {version_info}, ... ] }
    data = {}
    for v in all_versions:
        if v.key not in data:
            data[v.key] = []
        data[v.key].append({
            "version_id": v.version_id,
            "is_latest": v.is_latest,
            "is_delete_marker": v.is_delete_marker,
            "last_modified": v.last_modified,
            "size": v.size,
            "etag": v.etag
        })
    return data

@router.post("/api/ui/{bucket_name}/rollback")
async def ui_rollback(bucket_name: str, key: str, version_id: str, session: Session = Depends(get_session), user: str = Depends(get_current_user)):
    """
    Rollback (Rewind):
    1. Identify target version.
    2. Delete all versions created AFTER the target.
    3. Make target version latest.
    """
    # 2. Robust Strategy: Fetch all versions ordered by time (descending), same as UI.
    # Iterate: Delete everything until we hit the target.
    stmt_all = select(ObjectVersion).where(
        ObjectVersion.bucket_name == bucket_name,
        ObjectVersion.key == key
    ).order_by(ObjectVersion.last_modified.desc())
    
    all_versions = session.exec(stmt_all).all()
    
    target_found = False
    for ver in all_versions:
        if ver.version_id == version_id:
            # Reached Target
            target_found = True
            ver.is_latest = True
            session.add(ver)
            continue
            
        if not target_found:
            # These are "newer" versions (appear before target in DESC list)
            # Delete them
            if ver.storage_path:
                delete_physical_file(ver.storage_path)
            session.delete(ver)
        else:
            # These are "older" versions
            # Ensure they are not marked latest (just cleanup)
            if ver.is_latest:
                ver.is_latest = False
                session.add(ver)

    if not target_found:
         # Should catch this earlier, but just in case
         raise HTTPException(status_code=404, detail="Target version not found in history")
         
    session.commit()
    return {"status": "success", "message": f"Rolled back to {version_id}, newer versions deleted."}

# --- Bucket Operations ---

@router.put("/{bucket_name}")
async def create_bucket(bucket_name: str, session: Session = Depends(get_session)):
    stmt = select(Bucket).where(Bucket.name == bucket_name)
    existing = session.exec(stmt).first()
    if existing:
        # AWS returns 200 if you own it, 409 if others. We just return 409 for simplicity if it exists.
        raise HTTPException(status_code=409, detail="BucketAlreadyExists")
    
    bucket = Bucket(name=bucket_name, versioning_enabled=True) # Enable versioning by default for this demo
    session.add(bucket)
    session.commit()
    return Response(status_code=200)

@router.get("/{bucket_name}")
async def list_objects(
    bucket_name: str, 
    versions: bool = False,
    session: Session = Depends(get_session)
):
    # Check bucket
    stmt_bucket = select(Bucket).where(Bucket.name == bucket_name)
    bucket = session.exec(stmt_bucket).first()
    if not bucket:
        raise HTTPException(status_code=404, detail="NoSuchBucket")

    if versions:
        # List object versions
        stmt = select(ObjectVersion).where(ObjectVersion.bucket_name == bucket_name).order_by(ObjectVersion.key, ObjectVersion.last_modified.desc())
        results = session.exec(stmt).all()
        
        xml_parts = [
            f'<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
            f'<Name>{bucket_name}</Name>',
            f'<Prefix></Prefix>',
            f'<KeyMarker></KeyMarker>',
            f'<VersionIdMarker></VersionIdMarker>',
            f'<IsTruncated>false</IsTruncated>'
        ]
        
        for ver in results:
            if ver.is_delete_marker:
                xml_parts.append(f'<DeleteMarker><Key>{ver.key}</Key><VersionId>{ver.version_id}</VersionId><IsLatest>{str(ver.is_latest).lower()}</IsLatest><LastModified>{get_iso_timestamp(ver.last_modified)}</LastModified><Owner><ID>{Owner.ID}</ID><DisplayName>{Owner.DisplayName}</DisplayName></Owner></DeleteMarker>')
            else:
                xml_parts.append(f'<Version><Key>{ver.key}</Key><VersionId>{ver.version_id}</VersionId><IsLatest>{str(ver.is_latest).lower()}</IsLatest><LastModified>{get_iso_timestamp(ver.last_modified)}</LastModified><ETag>{ver.etag}</ETag><Size>{ver.size}</Size><Owner><ID>{Owner.ID}</ID><DisplayName>{Owner.DisplayName}</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Version>')
        
        xml_parts.append('</ListVersionsResult>')
        return generate_xml_response("".join(xml_parts))

    else:
        # List latest objects (hide delete markers)
        stmt = select(ObjectVersion).where(ObjectVersion.bucket_name == bucket_name, ObjectVersion.is_latest == True, ObjectVersion.is_delete_marker == False)
        results = session.exec(stmt).all()
        
        xml_parts = [
            f'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
            f'<Name>{bucket_name}</Name>',
            f'<Prefix></Prefix>',
            f'<Marker></Marker>',
            f'<MaxKeys>1000</MaxKeys>',
            f'<IsTruncated>false</IsTruncated>'
        ]
        
        for obj in results:
            xml_parts.append(f'<Contents><Key>{obj.key}</Key><LastModified>{get_iso_timestamp(obj.last_modified)}</LastModified><ETag>{obj.etag}</ETag><Size>{obj.size}</Size><Owner><ID>{Owner.ID}</ID><DisplayName>{Owner.DisplayName}</DisplayName></Owner><StorageClass>STANDARD</StorageClass></Contents>')
        
        xml_parts.append('</ListBucketResult>')
        return generate_xml_response("".join(xml_parts))

# --- Object Operations ---

@router.put("/{bucket_name}/{key:path}")
async def put_object(
    bucket_name: str, 
    key: str, 
    request: Request,
    session: Session = Depends(get_session)
):
    # Determine bucket
    stmt_bucket = select(Bucket).where(Bucket.name == bucket_name)
    bucket = session.exec(stmt_bucket).first()
    if not bucket:
        raise HTTPException(status_code=404, detail="NoSuchBucket")
    
    # Read content
    # For small demo using UploadFile is tricky with path params. 
    # Standard S3 PUT sends body as raw bytes. FastAPI Request.stream() is best, 
    # but for simplicity let's read into memory or spool.
    body = await request.body()
    content_type = request.headers.get("content-type", "application/octet-stream")
    
    # Create temp file object to reuse existing storage logic
    from io import BytesIO
    class HashableFile:
        def __init__(self, data):
             self.file = BytesIO(data)
    
    file_mock = HashableFile(body)
    
    # Calculate Props
    # Calculate ETag
    hash_md5 = calculate_etag(file_mock) # This resets cursor
    size = len(body)
    
    version_id = str(uuid.uuid4())
    
    # Save physical file
    # Storage logic expects UploadFile-like, let's wrap it properly or adjust storage.
    # storage.save_file uses 'shutil.copyfileobj(file_obj.file, buffer)'
    # file_mock has .file which is BytesIO, which works with copyfileobj
    
    storage_path = save_file(file_mock, bucket_name, key, version_id)
    
    # Handle versioning status
    # Update old latest to not latest
    stmt_old = select(ObjectVersion).where(ObjectVersion.bucket_name == bucket_name, ObjectVersion.key == key, ObjectVersion.is_latest == True)
    old_latest = session.exec(stmt_old).first()
    if old_latest:
        old_latest.is_latest = False
        session.add(old_latest)
    
    new_obj = ObjectVersion(
        bucket_name=bucket_name,
        key=key,
        version_id=version_id,
        is_latest=True,
        is_delete_marker=False,
        size=size,
        etag=hash_md5,
        content_type=content_type,
        storage_path=storage_path
    )
    session.add(new_obj)
    session.commit()
    
    headers = {
        "x-amz-version-id": version_id,
        "ETag": hash_md5
    }
    return Response(status_code=200, headers=headers)

from fastapi.responses import StreamingResponse

@router.get("/{bucket_name}/{key:path}")
async def get_object(
    bucket_name: str, 
    key: str, 
    versionId: Optional[str] = None,
    session: Session = Depends(get_session)
):
    stmt = select(ObjectVersion).where(ObjectVersion.bucket_name == bucket_name, ObjectVersion.key == key)
    
    if versionId:
        stmt = stmt.where(ObjectVersion.version_id == versionId)
    else:
        stmt = stmt.where(ObjectVersion.is_latest == True)
        
    obj = session.exec(stmt).first()
    
    if not obj:
        raise HTTPException(status_code=404, detail="NoSuchKey")
    
    if obj.is_delete_marker:
         raise HTTPException(status_code=404, detail="NoSuchKey") # Delete marker behaves like 404 for GET
         
    file_stream = get_file_content(obj.storage_path)
    if not file_stream:
         raise HTTPException(status_code=500, detail="InternalError: File missing on disk")
    
    # Simple streaming response
    def iterfile():  
        yield from file_stream # This is synchronous generator, for prod use aiofiles or ThreadPool
        file_stream.close()
        
    headers = {
        "x-amz-version-id": obj.version_id,
        "ETag": obj.etag,
        "Content-Type": obj.content_type,
        "Content-Length": str(obj.size)
    }
    
    return StreamingResponse(content=iterfile(), headers=headers)

@router.delete("/{bucket_name}/{key:path}")
async def delete_object(
    bucket_name: str, 
    key: str, 
    versionId: Optional[str] = None,
    session: Session = Depends(get_session)
):
    if versionId:
        # Delete specific version
        stmt = select(ObjectVersion).where(ObjectVersion.bucket_name == bucket_name, ObjectVersion.key == key, ObjectVersion.version_id == versionId)
        obj = session.exec(stmt).first()
        if obj:
            # If we delete the latest version, we need to promote the previous version to latest?
            # S3 behavior: Deleting a specific version just removes it. It does NOT automatically promote others usually? 
            # Actually, if you delete the current version (latest), the next most recent becomes latest? NO.
            # Wait, if you delete a version ID, it's just gone.
            # If you simple DELETE without version ID, you insert a DELETE MARKER.
            
            # Implementation for this demo:
            # If deleting specific version:
            if obj.is_latest:
                # If we are deleting the latest version, we must make the next recent one latest
                # Find next recent
                 stmt_next = select(ObjectVersion).where(
                     ObjectVersion.bucket_name == bucket_name, 
                     ObjectVersion.key == key,
                     ObjectVersion.version_id != versionId
                 ).order_by(ObjectVersion.last_modified.desc())
                 next_latest = session.exec(stmt_next).first()
                 if next_latest:
                     next_latest.is_latest = True
                     session.add(next_latest)
            
            # Physical delete
            if obj.storage_path:
                delete_physical_file(obj.storage_path)
            
            session.delete(obj)
            session.commit()
            return Response(status_code=204)
        else:
             return Response(status_code=204) # S3 returns success even if not found
    else:
        # Simple Delete: Insert Delete Marker
        # Check bucket exists
        stmt_bucket = select(Bucket).where(Bucket.name == bucket_name)
        bucket = session.exec(stmt_bucket).first()
        if not bucket:
            raise HTTPException(status_code=404, detail="NoSuchBucket")

        # Update old latest
        stmt_old = select(ObjectVersion).where(ObjectVersion.bucket_name == bucket_name, ObjectVersion.key == key, ObjectVersion.is_latest == True)
        old_latest = session.exec(stmt_old).first()
        if old_latest:
            old_latest.is_latest = False
            session.add(old_latest)
            
        version_id = str(uuid.uuid4())
        delete_marker = ObjectVersion(
            bucket_name=bucket_name,
            key=key,
            version_id=version_id,
            is_latest=True,
            is_delete_marker=True
        )
        session.add(delete_marker)
        session.commit()
        
        return Response(status_code=204, headers={"x-amz-version-id": version_id, "x-amz-delete-marker": "true"})

