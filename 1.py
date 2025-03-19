from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.auth.transport.requests import Request
import os

# Настройки
SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]
CLIENT_SECRETS_FILE = "client_secret.json"
TOKEN_FILE = "token.json"

# Авторизация
def get_authenticated_service():
    credentials = None
    if os.path.exists(TOKEN_FILE):
        try:
            credentials = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
            if credentials.expired and credentials.refresh_token:
                credentials.refresh(Request())
        except Exception as e:
            print(f"Ошибка при загрузке токена: {e}")
            credentials = None
    if not credentials or not credentials.valid:
        flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
        credentials = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w") as token:
            token.write(credentials.to_json())
    return build("youtube", "v3", credentials=credentials)

# Функция загрузки видео
def upload_video(youtube, file_path, title, description, tags, privacy_status="private"):
    if not title or title.strip() == "":
        title = "Тестовое видео"
    if isinstance(tags, str):
        tags_list = tags.split(", ")
    else:
        tags_list = list(tags)
    body = {
        "snippet": {
            "title": title,
            "description": description,
            "tags": tags_list,
            "categoryId": "22"
        },
        "status": {
            "privacyStatus": privacy_status
        }
    }
    media = MediaFileUpload(file_path, chunksize=-1, resumable=True)
    request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
    response = request.execute()
    print(f"Видео загружено с ID: {response['id']}")

# Запуск
youtube = get_authenticated_service()
file_path = "1804048698_part1.mp4"
title = "Тянукус! День 6. Страна волкодавия. Часть 1 (13.03.2023)"
description = "Gothic, Just Chatting"
tags = "unuasha, юняша, twitch, стрим, Gothic, Just Chatting"
privacy_status = "private"

upload_video(youtube, file_path, title, description, tags, privacy_status)