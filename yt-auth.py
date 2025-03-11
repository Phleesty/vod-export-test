import os
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow

# Путь к файлу с учетными данными (переименуй свой JSON-файл в client_secret.json)
CLIENT_SECRETS_FILE = 'client_secret.json'
# Путь к файлу с токеном
TOKEN_FILE = 'token.json'
# Область доступа для загрузки видео
SCOPES = ['https://www.googleapis.com/auth/youtube.upload']

# Функция для авторизации
def get_authenticated_service():
    if os.path.exists(TOKEN_FILE):
        # Если токен уже есть, используем его
        credentials = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    else:
        # Если токена нет, запускаем интерактивную авторизацию
        flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
        credentials = flow.run_local_server(port=0)
        # Сохраняем токен для будущих запусков
        with open(TOKEN_FILE, 'w') as token:
            token.write(credentials.to_json())
    return build('youtube', 'v3', credentials=credentials)

# Функция для загрузки видео
def upload_video(youtube, video_file, title, description, tags, category_id, privacy_status):
    body = {
        'snippet': {
            'title': title,
            'description': description,
            'tags': tags,
            'categoryId': category_id
        },
        'status': {
            'privacyStatus': privacy_status  # 'public', 'private' или 'unlisted'
        }
    }
    # Загружаем видео
    media = MediaFileUpload(video_file, chunksize=-1, resumable=True)
    request = youtube.videos().insert(
        part='snippet,status',
        body=body,
        media_body=media
    )
    response = request.execute()
    return response

# Основной код
if __name__ == '__main__':
    youtube = get_authenticated_service()
    video_file = 'F:/youtube test/1.mp4'  # Укажи путь к своему видео
    title = 'Тестовое видео'
    description = 'Это тестовое видео, загруженное через Python.'
    tags = ['тест', 'видео', 'python']
    category_id = '22'  # Категория "People & Blogs" (список ID: https://developers.google.com/youtube/v3/docs/videoCategories/list)
    privacy_status = 'private'  # Статус видео
    response = upload_video(youtube, video_file, title, description, tags, category_id, privacy_status)
    print(f"Видео загружено! ID: {response['id']}")