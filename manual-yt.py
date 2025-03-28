from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import os

# Настройки
SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]
CLIENT_SECRETS_FILE = "client_secret.json"
TOKEN_FILE = "token.json"

# Авторизация
def get_authenticated_service():
    if os.path.exists(TOKEN_FILE):
        credentials = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    else:
        flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
        credentials = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w") as token:
            token.write(credentials.to_json())
    return build("youtube", "v3", credentials=credentials)

# Функция загрузки видео
def upload_video(youtube, file_path, title, description, tags, privacy_status="private"):
    # Проверка заголовка
    if not title or title.strip() == "":
        title = "Тестовое видео"  # Заголовок по умолчанию

    # Обработка тегов
    if isinstance(tags, str):
        tags_list = tags.split(", ")
    else:
        tags_list = list(tags)  # Преобразование кортежа или списка в список

    body = {
        "snippet": {
            "title": title,
            "description": description,
            "tags": tags_list,
            "categoryId": "22"  # Категория "Люди и блоги"
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
file_path = "group_0.mp4"
title = "ПEРВОЕ ПРОХОЖДЕНИЕ БЕЗ ПОДСКАЗОК БЕЗ КУЗНЕЦА БЕЗ ОСВАЛЬДА БЕЗ ТОРГОВЦА ФУЛЛ ФОКУС (18.02.2025)"  # Убедитесь, что заголовок не пустой
description = "Just Chatting, Dark Souls: Remastered"
tags = "unuasha, юняша, twitch, стрим, Just Chatting, Dark Souls: Remastered"  # Теги как строка
privacy_status = "private"  # Статус приватности

upload_video(youtube, file_path, title, description, tags, privacy_status)