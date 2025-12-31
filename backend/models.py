from datetime import datetime
from typing import Optional, List
from sqlmodel import SQLModel, Field, Relationship

class Bucket(SQLModel, table=True):
    name: str = Field(primary_key=True, index=True)
    creation_date: datetime = Field(default_factory=datetime.utcnow)
    versioning_enabled: bool = Field(default=False)

    object_versions: List["ObjectVersion"] = Relationship(back_populates="bucket_conn")

class ObjectVersion(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    bucket_name: str = Field(foreign_key="bucket.name", index=True)
    key: str = Field(index=True)
    version_id: str = Field(index=True)
    is_latest: bool = Field(default=True)
    is_delete_marker: bool = Field(default=False)
    size: int = Field(default=0)
    etag: str = Field(default="")
    last_modified: datetime = Field(default_factory=datetime.utcnow)
    content_type: str = Field(default="application/octet-stream")
    storage_path: Optional[str] = Field(default=None) # Path on disk relative to storage root

    bucket_conn: Bucket = Relationship(back_populates="object_versions")

class Owner:
    ID = "00000000000000000000000000000000"
    DisplayName = "antigravity"
