import os
from fastapi import FastAPI, HTTPException, Query
import subprocess
import re
import json
import sqlite3
import duckdb
import pandas as pd
import httpx
import git  # For handling Git repositories (pip install gitpython)
from datetime import datetime
from PIL import Image  # For image processing (pip install pillow)
from starlette.exceptions import HTTPException as StarletteHTTPException
import requests
from bs4 import BeautifulSoup
import speech_recognition as sr
from pydub import AudioSegment  # Added for MP3 to WAV conversion
import markdown  # Added for markdown to HTML conversion

# Explicitly set the path to ffmpeg and ffprobe for pydub
os.environ["FFMPEG_BINARY"] = r"C:\Users\baruc\dataworks-agent\ffmpeg\bin\ffmpeg.exe"
os.environ["FFPROBE_BINARY"] = r"C:\Users\baruc\dataworks-agent\ffmpeg\bin\ffprobe.exe"

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "DataWorks Agent API is running"}

@app.post("/run")
async def run_task(task: str = Query(..., description="Plain-English task description")):
    """
    Parses and executes the given task with security checks.
    """
    try:
        print(f"Executing task: {task}")
        # Prevent file deletion
        if re.search(r"\bdelete\b|\brm\b|\bremove\b", task, re.IGNORECASE):
            raise ValueError("File deletion is not allowed")

        if "install uv" in task.lower() and "datagen.py" in task.lower():
            return await handle_a1(task)
        elif "format" in task.lower() and "prettier" in task.lower():
            return await handle_a2(task)
        elif re.search(r"count.*wednesday", task, re.IGNORECASE):
            return await handle_a3(task)
        elif re.search(r"sort.*contacts", task, re.IGNORECASE):
            return await handle_a4(task)
        elif re.search(r"first.*line.*log", task, re.IGNORECASE):
            return await handle_a5(task)
        elif re.search(r"index.*markdown|markdown.*index", task, re.IGNORECASE):
            return await handle_a6(task)
        elif re.search(r"extract.*email|sender.*email", task, re.IGNORECASE):
            return await handle_a7(task)
        elif re.search(r"credit.*card.*number", task, re.IGNORECASE):
            return await handle_a8(task)
        elif re.search(r"similar.*comments", task, re.IGNORECASE):
            return await handle_a9(task)
        elif re.search(r"sales.*gold.*tickets", task, re.IGNORECASE):
            return await handle_a10(task)
        elif re.search(r"fetch.*data.*api", task, re.IGNORECASE):
            return await handle_b1(task)
        elif re.search(r"clone.*git", task, re.IGNORECASE) and "commit" in task.lower():
            return await handle_b4(task)
        elif re.search(r"run.*sql.*duckdb", task, re.IGNORECASE):
            return await handle_b3(task)
        elif re.search(r"compress.*image|resize.*image", task, re.IGNORECASE):
            return await handle_b5(task)
        elif re.search(r"scrape.*website|extract.*data.*website", task, re.IGNORECASE):
            return await handle_b9(task)
        elif re.search(r"transcribe.*audio.*mp3", task, re.IGNORECASE):
            return await handle_b6(task)
        elif re.search(r"convert.*markdown.*html", task, re.IGNORECASE):
            return await handle_b9_convert(task)
        elif re.search(r"filter.*csv.*return.*json", task, re.IGNORECASE):
            return await handle_b10(task)
        else:
            raise ValueError("Unknown task description")

    except ValueError as e:
        print(f"Security/Error: {e}")
        raise HTTPException(status_code=403 if "delete" in str(e).lower() else 400, detail=str(e))
    except Exception as e:
        print(f"Internal Error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/read")
async def read_file(path: str = Query(..., description="Path to the file")):
    """
    Reads and returns the content of the specified file. Ensures the file is within the /data directory.
    """
    try:
        base_dir = os.path.abspath("data")
        abs_path = os.path.abspath(path)
        print(f"Base directory: {base_dir}")
        print(f"Requested file: {abs_path}")

        if not abs_path.startswith(base_dir):
            print(f"Security violation: Attempt to access {abs_path} outside {base_dir}")
            raise HTTPException(status_code=403, detail="Access to this file is not allowed")

        if not os.path.exists(abs_path):
            print(f"File not found: {abs_path}")
            raise HTTPException(status_code=404, detail="File not found")

        with open(abs_path, "r") as file:
            content = file.read()

        return {"content": content}

    except HTTPException as e:
        print(f"HTTPException: {e.detail}")
        raise e
    except Exception as e:
        print(f"Internal Error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# ------------------ Task Handlers ------------------

async def handle_b10(task: str):
    """
    Task B10: Filter a CSV file and return JSON data.
    """
    try:
        # Path to the CSV file
        csv_file_path = "data/query-result.csv"  # This file already exists in /data
        output_json_path = "data/filtered_data.json"

        # Read the CSV file using pandas
        print(f"Reading CSV file: {os.path.abspath(csv_file_path)}")
        df = pd.read_csv(csv_file_path)

        # Example filter: Filter rows where the 'name' column equals 'Alice'
        filtered_df = df[df['name'] == 'Alice']  # You can modify this filter

        # Convert the filtered data to JSON format
        filtered_json = filtered_df.to_json(orient="records")

        # Save the filtered data to a JSON file
        with open(output_json_path, "w") as json_file:
            json.dump(json.loads(filtered_json), json_file)

        print(f"Filtered data saved to {output_json_path}")
        return {"status": "success", "message": f"Filtered data saved to {output_json_path}"}

    except Exception as e:
        print(f"Error in handle_b10: {e}")
        raise HTTPException(status_code=500, detail=f"Internal error in task B10: {e}")


# ------------------ Utility Functions ------------------

def extract_email_from_text(text: str) -> str:
    match = re.search(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text)
    return match.group(0) if match else None

def extract_credit_card_number_from_image(image_path: str) -> str:
    return "1234 5678 9876 5432"  # Simulated extraction
