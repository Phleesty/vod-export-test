import subprocess
import os
import argparse
import logging
import math
from datetime import datetime
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow

# Константы
FFMPEG_PATH = "/usr/bin/ffmpeg"
FFPROBE_PATH = "/usr/bin/ffprobe"
CLIENT_SECRETS_FILE = "client_secret.json"
TOKEN_FILE = "token.json"
SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]
# Максимальная длительность видео для загрузки: 11 часов 58 минут = 43080 секунд
MAX_ALLOWED_DURATION = 11 * 3600 + 58 * 60

# Настройка логирования
logging.basicConfig(
    filename="youtube_uploader.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def setup_credentials():
    """
    Настраивает учетные данные YouTube.
    Если файлы client_secret.json и token.json существуют, спрашиваем, использовать их или задать новые.
    """
    if os.path.exists(CLIENT_SECRETS_FILE) and os.path.exists(TOKEN_FILE):
        choice = input("Продолжить с сохраненными настройками? (y/n): ").strip().lower()
        if choice == 'y':
            return
        else:
            logging.info("Настройка новых учетных данных...")
    client_secret = input("Введите содержимое client_secret.json (или 'n' для пропуска): ").strip()
    if client_secret.lower() != 'n':
        with open(CLIENT_SECRETS_FILE, "w") as f:
            f.write(client_secret)
        logging.info(f"{CLIENT_SECRETS_FILE} успешно сохранен.")
    token = input("Введите содержимое token.json (или 'n' для пропуска): ").strip()
    if token.lower() != 'n':
        with open(TOKEN_FILE, "w") as f:
            f.write(token)
        logging.info(f"{TOKEN_FILE} успешно сохранен.")

def get_video_duration(video_file):
    command = [
        FFPROBE_PATH, "-v", "error", "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1", video_file
    ]
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    output = result.stdout.strip()
    if not output:
        raise ValueError(f"Не удалось получить длительность для файла {video_file}. Проверьте, существует ли файл и корректно ли установлен ffprobe.")
    return float(output)

def concatenate_videos(video_files, output_file):
    """
    Объединяет несколько видеофайлов в один с помощью ffmpeg.
    """
    with open("concat_list.txt", "w") as f:
        for video in video_files:
            f.write(f"file '{os.path.abspath(video)}'\n")
    command = [FFMPEG_PATH, "-f", "concat", "-safe", "0", "-i", "concat_list.txt", "-c", "copy", output_file]
    subprocess.run(command, check=True)
    os.remove("concat_list.txt")
    logging.info(f"Видео объединено в {output_file}")
    return output_file

def split_single_video(video_file):
    """
    Разбивает видео, если его длительность превышает MAX_ALLOWED_DURATION.
    Если длительность меньше лимита – возвращает список с одним элементом.
    Логика:
      - Если видео делится на 2 части, первая часть округляется вверх до целых часов (но не превышает лимит).
      - Если частей больше, первые (n-1) частей получают базовую длительность в секундах,
        а последняя – остаток.
    """
    duration = get_video_duration(video_file)
    if duration <= MAX_ALLOWED_DURATION:
        return [video_file]

    parts = int(math.ceil(duration / MAX_ALLOWED_DURATION))
    split_points = []
    if parts == 2:
        first_part = math.ceil((duration / 2) / 3600) * 3600
        if first_part > MAX_ALLOWED_DURATION:
            first_part = MAX_ALLOWED_DURATION
        split_points.append(first_part)
    else:
        base = int(math.floor((duration / parts) / 3600)) * 3600
        for _ in range(parts - 1):
            split_points.append(base)

    part_files = []
    start_time = 0
    part_num = 1
    for sp in split_points:
        part_file = f"{video_file[:-4]}_part{part_num}.mp4"
        logging.info(f"Разбиваю {video_file} на часть {part_num} (длительность: {sp/3600:.2f} ч, начало: {start_time} сек)")
        command = [
            FFMPEG_PATH, "-i", video_file, "-ss", str(start_time),
            "-t", str(sp), "-c", "copy", part_file
        ]
        subprocess.run(command, check=True)
        part_files.append(part_file)
        start_time += sp
        part_num += 1
    if start_time < duration:
        part_file = f"{video_file[:-4]}_part{part_num}.mp4"
        logging.info(f"Создаю последнюю часть {part_num} (длительность: {(duration - start_time)/3600:.2f} ч, начало: {start_time} сек)")
        command = [
            FFMPEG_PATH, "-i", video_file, "-ss", str(start_time),
            "-t", str(duration - start_time), "-c", "copy", part_file
        ]
        subprocess.run(command, check=True)
        part_files.append(part_file)
    return part_files

def get_authenticated_youtube_service():
    """
    Возвращает аутентифицированный сервис для работы с YouTube API.
    """
    if os.path.exists(TOKEN_FILE):
        credentials = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    else:
        flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
        credentials = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w") as token:
            token.write(credentials.to_json())
    return build("youtube", "v3", credentials=credentials)

def upload_to_youtube(video_file, title, description, tags):
    """
    Загружает видео на YouTube с заданными параметрами.
    """
    logging.info(f"Загружаю {video_file} на YouTube как '{title}'...")
    youtube = get_authenticated_youtube_service()
    body = {
        "snippet": {
            "title": title,
            "description": description,
            "tags": [tag.strip() for tag in tags.split(",") if tag.strip()],
            "categoryId": "22"
        },
        "status": {
            "privacyStatus": "private"
        }
    }
    media = MediaFileUpload(video_file, chunksize=-1, resumable=True)
    request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
    response = request.execute()
    logging.info(f"Видео {video_file} успешно загружено.")
    print(f"Видео {video_file} успешно загружено.")

def add_part_to_title(title, part_number):
    """
    Добавляет номер части в заголовок видео.
    """
    last_open_paren = title.rfind("(")
    if last_open_paren == -1:
        return f"{title}. Часть {part_number}"
    date_part = title[last_open_paren:]
    main_title = title[:last_open_paren].strip()
    if main_title and main_title[-1] in ".!?":
        new_main_title = f"{main_title} Часть {part_number}"
    else:
        new_main_title = f"{main_title}. Часть {part_number}"
    return f"{new_main_title} {date_part}"

def main():
    setup_credentials()

    parser = argparse.ArgumentParser(description="Видео сплиттер и загрузчик на YouTube")
    parser.add_argument("--files", nargs="+", help="Пути к видеофайлам", required=False)
    parser.add_argument("--merge", action="store_true", help="Объединить видеофайлы перед обработкой")
    args = parser.parse_args()

    # Получаем список видеофайлов для обработки
    if args.files:
        video_files = args.files
    else:
        files_input = input("Введите пути к видеофайлам, разделенные пробелом: ")
        video_files = files_input.split()

    # Если указан флаг merge и файлов больше одного, объединяем их
    if args.merge and len(video_files) > 1:
        merged_file = "merged_video.mp4"
        concatenate_videos(video_files, merged_file)
        video_files = [merged_file]

    # Запрашиваем параметры для загрузки
    title = input("Введите заголовок для видео: ").strip()
    description = input("Введите описание для видео: ").strip()
    tags = input("Введите теги для видео (через запятую): ").strip()

    # Обрабатываем каждый видеофайл
    for video_file in video_files:
        duration = get_video_duration(video_file)
        print(f"Длительность {video_file}: {duration/3600:.2f} часов")
        if duration > MAX_ALLOWED_DURATION:
            print(f"Видео {video_file} превышает лимит, оно будет разделено.")
            parts = split_single_video(video_file)
            for i, part in enumerate(parts, start=1):
                part_title = add_part_to_title(title, i)
                upload_to_youtube(part, part_title, description, tags)
        else:
            upload_to_youtube(video_file, title, description, tags)

if __name__ == "__main__":
    main()
