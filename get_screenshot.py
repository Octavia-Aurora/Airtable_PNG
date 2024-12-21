from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import FileResponse
import requests
import os

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
                        return file_path
    return None


def save_file(url, file_name):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    file_path = os.path.join(OUTPUT_DIR, file_name)

    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        with open(file_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
    return file_path


@app.get("/get-file/")
async def get_file(field_name: str = Query(..., description="Name of the Airtable field containing the file")):
    """
    Endpoint to retrieve a file from Airtable and serve it to the user.
    """
    try:
        file_path = download_file_from_record(BASE_ID, TABLE_NAME, field_name)
        if file_path and os.path.exists(file_path):
            # Return the file for download
            return FileResponse(file_path, media_type="application/octet-stream", filename=os.path.basename(file_path))
        else:
            raise HTTPException(status_code=404, detail="File not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
