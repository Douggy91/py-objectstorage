from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from backend.database import create_db_and_tables
from backend.s3_routes import router as s3_routes
import uvicorn
import os
## gemini 추가
from fastapi.responses import RedirectResponse 
from fastapi import Request

app = FastAPI(title="Python ObjectStorage S3")

# Initialize DB on startup
@app.on_event("startup")
def on_startup():
    create_db_and_tables()

@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/_ui")

# API Handling
# We mount s3_routes at root for S3 compatibility

frontend_path = "d:/WorkShop/Practice/PythonWorkSpace/ObjectStorage/frontend"
os.makedirs(frontend_path, exist_ok=True)
app.mount("/_ui", StaticFiles(directory=frontend_path, html=True), name="ui")

app.include_router(s3_routes, prefix="/_s3")

# Static files for GUI
# Provide a 'gui' path or serve at root if not colliding with buckets? 
# S3 buckets are at root.
# Strategy: Serve GUI at /_console/ and redirect / to /_console/ index if raw browser visit?
# Or just separate port? 
# Let's put GUI at /ui
# Note: S3 buckets usually subdomain or path. Path style: /bucket/key.
# So /ui might be a valid bucket name.
# We will reserve 'ui' as a bucket name or just accept the collision risk for this demo.
# Let's use `/_ui` to be safer.

if __name__ == "__main__":
    uvicorn.run("backend.main:app", host="0.0.0.0", port=8000, reload=True)
