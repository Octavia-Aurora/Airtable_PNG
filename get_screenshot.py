from fastapi import FastAPI, Query, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse, FileResponse
import os
import requests
import time

# Initialize FastAPI app
app = FastAPI()

# Load Airtable configurations from environment variables
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID = os.getenv("BASE_ID")
TABLE_NAME = os.getenv("TABLE_NAME")
OUTPUT_DIR = "downloads"

# Ensure all necessary environment variables are set
if not AIRTABLE_API_KEY or not BASE_ID or not TABLE_NAME:
    raise RuntimeError("Missing required environment variables for Airtable configuration")


def download_file_from_record(base_id, table_name, field_name):
    """
    Downloads a file from Airtable and saves it locally.
    """
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
    url = f"https://api.airtable.com/v0/{base_id}/{table_name}"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        records = response.json().get("records", [])
        for record in records:
            field_data = record.get("fields", {}).get(field_name, [])
            if isinstance(field_data, list) and len(field_data) > 0:
                for attachment in field_data:
                    file_url = attachment.get("url")
                    file_name = attachment.get("filename", "unknown_file")
                    if file_url:
                        file_path = save_file(file_url, file_name)
                        return file_path, file_name
    return None, None


def save_file(url, file_name):
    """
    Saves the file locally after downloading it.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    file_path = os.path.join(OUTPUT_DIR, file_name)

    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        with open(file_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
    return file_path


def delete_file(file_path: str):
    """
    Deletes the file from the local server.
    """
    time.sleep(120)  # Wait for 2 minutes (120 seconds)
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"Deleted file: {file_path}")


@app.get("/get-file/")
async def get_file_url(field_name: str = Query(..., description="Name of the Airtable field containing the file"),
                       background_tasks: BackgroundTasks = BackgroundTasks()):
    """
    Endpoint to retrieve a file from Airtable and provide a public URL to access it.
    """
    try:
        file_path, file_name = download_file_from_record(BASE_ID, TABLE_NAME, field_name)
        if file_path and os.path.exists(file_path):
            # Add the file deletion task
            background_tasks.add_task(delete_file, file_path)

            # Replace `your-app-name.onrender.com` with your Render URL
            public_url = f"https://your-app-name.onrender.com/files/{file_name}"
            return {
                "file_name": file_name,
                "file_url": public_url,
                "message": "File will be deleted automatically after 2 minutes."
            }
        else:
            return JSONResponse(
                status_code=404,
                content={"error": "File not found", "message": f"No file found for field '{field_name}'."}
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/files/{file_name}")
async def serve_file(file_name: str):
    """
    Serves a file via a public URL.
    """
    file_path = os.path.join(OUTPUT_DIR, file_name)
    if os.path.exists(file_path):
        return FileResponse(file_path, media_type="application/octet-stream", filename=file_name)
    else:
        raise HTTPException(status_code=404, detail="File not found")
