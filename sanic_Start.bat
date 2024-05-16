@echo off
cd D:\Teum4U\Hesderim_2
start "" "D:\Teum4U\Hesderim_2\env_geoserver\.venv\Scripts\python.exe" -m uvicorn main:app --host 127.0.0.1 --port 8000 --workers 4

 