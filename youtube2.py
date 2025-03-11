import pandas as pd
import subprocess
import os
import argparse
import logging
import requests
import zipfile
import shutil
from datetime import datetime
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow

# Пути к инструментам и файлам
TWITCH_DOWNLOADER_PATH = "./TwitchDownloaderCLI/TwitchDownloaderCLI"
FFMPEG_PATH = "/usr/bin/ffmpeg"
FFPROBE_PATH = "/usr/bin/ffprobe"
STREAMS_FILE = "streams.xlsx"
CLIENT_SECRETS_FILE = "client_secret.json"
TOKEN_FILE = "token.json"
SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]

# Настройка логирования
logging.basicConfig(
    filename="youtube_uploader.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Функция для автоматической настройки окружения
def setup_environment():
    if not shutil.which("ffmpeg"):
        logging.warning("FFmpeg не установлен. Установите его командой: 'sudo apt install ffmpeg'")
        print("FFmpeg не установлен. Установите его командой: 'sudo apt install ffmpeg'")
        exit(1)
    if not os.path.exists("./TwitchDownloaderCLI"):
        logging.info("Скачиваю TwitchDownloaderCLI...")
        print("Скачиваю TwitchDownloaderCLI...")
        url = "https://github.com/lay295/TwitchDownloader/releases/download/1.55.2/TwitchDownloaderCLI-1.55.2-Linux-x64.zip"
        response = requests.get(url)
        with open("TwitchDownloaderCLI.zip", "wb") as f:
            f.write(response.content)
        with zipfile.ZipFile("TwitchDownloaderCLI.zip", "r") as zip_ref:
            zip_ref.extractall("./TwitchDownloaderCLI")
        os.remove("TwitchDownloaderCLI.zip")
        logging.info("TwitchDownloaderCLI успешно установлен.")
        print("TwitchDownloaderCLI успешно установлен.")

# Функция для настройки учетных данных
def setup_credentials():
    if os.path.exists(CLIENT_SECRETS_FILE) and os.path.exists(TOKEN_FILE):
        choice = input("Хотите продолжить с сохраненными настройками? (y/n): ").strip().lower()
        if choice == 'y':
            return
        else:
            logging.info("Настройка новых учетных данных...")
            print("Настройка новых учетных данных...")

    # Настройка client_secret.json
    client_secret = input("Введите ваш client secrets file (или 'n' для пропуска): ").strip()
    if client_secret.lower() != 'n':
        with open(CLIENT_SECRETS_FILE, "w") as f:
            f.write(client_secret)
        logging.info(f"{CLIENT_SECRETS_FILE} успешно сохранен.")
        print(f"{CLIENT_SECRETS_FILE} успешно сохранен.")
    else:
        logging.info(f"Пропуск настройки {CLIENT_SECRETS_FILE}.")
        print(f"Пропуск настройки {CLIENT_SECRETS_FILE}.")

    # Настройка token.json
    token = input("Введите ваш токен файл (или 'n' для пропуска): ").strip()
    if token.lower() != 'n':
        with open(TOKEN_FILE, "w") as f:
            f.write(token)
        logging.info(f"{TOKEN_FILE} успешно сохранен.")
        print(f"{TOKEN_FILE} успешно сохранен.")
    else:
        logging.info(f"Пропуск настройки {TOKEN_FILE}.")
        print(f"Пропуск настройки {TOKEN_FILE}.")

# Функция для скачивания видео с Twitch
def download_twitch_video(video_url, output_file):
    start_time = datetime.now()
    video_id = video_url.split("/")[-1] if "twitch.tv" in video_url else video_url
    command = [TWITCH_DOWNLOADER_PATH, "videodownload", "--id", video_id, "-o", output_file, "--threads", "20"]
    logging.debug(f"Выполняю команду: {' '.join(command)}")
    print(f"Начинаю скачивать файл {output_file}...")
    subprocess.run(command, check=True)
    end_time = datetime.now()
    download_time = (end_time - start_time).total_seconds()
    file_size = os.path.getsize(output_file) / (1024 * 1024)  # Размер в МБ
    speed = file_size / download_time if download_time > 0 else 0
    msg = f"Файл {output_file} ({file_size:.2f} МБ) скачан за {int(download_time // 60)} мин {int(download_time % 60)} сек, скорость: {speed:.2f} МБ/с"
    logging.info(msg)
    print(msg)

# Функция для объединения видео
def concatenate_videos(video_files, output_file):
    logging.info("Объединяю видео...")
    print("Объединяю видео...")
    with open("concat_list.txt", "w") as f:
        for video_file in video_files:
            f.write(f"file '{video_file}'\n")
    command = [FFMPEG_PATH, "-f", "concat", "-safe", "0", "-i", "concat_list.txt", "-c", "copy", output_file]
    logging.debug(f"Выполняю команду: {' '.join(command)}")
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    os.remove("concat_list.txt")
    logging.info(f"Видео объединено в {output_file}")
    print(f"Видео объединено в {output_file}")

# Функция для получения длительности видео
def get_video_duration(video_file):
    command = [FFPROBE_PATH, "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", video_file]
    logging.debug(f"Выполняю команду: {' '.join(command)}")
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return float(result.stdout.strip())

# Функция для разделения видео на части
def split_single_video(video_file, max_duration=12*3600, safe_duration=10*3600):
    duration = get_video_duration(video_file)
    parts = []
    start_time = 0
    part_num = 1

    while start_time < duration:
        part_duration = min(safe_duration, duration - start_time)
        part_file = f"{video_file[:-4]}_part{part_num}.mp4"
        command = [FFMPEG_PATH, "-i", video_file, "-ss", str(start_time), "-t", str(part_duration), "-c", "copy", part_file]
        logging.debug(f"Выполняю команду: {' '.join(command)}")
        logging.info(f"Разделяю {video_file} на часть {part_num}...")
        print(f"Разделяю {video_file} на часть {part_num}...")
        process = subprocess.Popen(command, stderr=subprocess.PIPE, universal_newlines=True)
        for line in process.stderr:
            if "time=" in line:
                logging.debug(f"Прогресс разделения части {part_num}: {line.strip()}")
                print(f"\rПрогресс разделения части {part_num}: {line.strip()}", end="")
        process.wait()
        if process.returncode == 0:
            parts.append(part_file)
            start_time += part_duration
            part_num += 1
        else:
            logging.error(f"Ошибка при разделении {video_file} на часть {part_num}")
            break
    print()  # Перевод строки после прогресса
    return parts

# Авторизация в YouTube API
def get_authenticated_youtube_service():
    if os.path.exists(TOKEN_FILE):
        credentials = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    else:
        flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
        credentials = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w") as token:
            token.write(credentials.to_json())
    return build("youtube", "v3", credentials=credentials)

# Функция для загрузки видео на YouTube
def upload_to_youtube(video_file, title, description, tags):
    logging.info(f"Загружаю {video_file} на YouTube как '{title}'...")
    print(f"Загружаю {video_file} на YouTube как '{title}'...")
    start_time = datetime.now()
    youtube = get_authenticated_youtube_service()
    body = {
        "snippet": {
            "title": title,
            "description": description,
            "tags": tags.split(", "),
            "categoryId": "22"  # Категория "People & Blogs"
        },
        "status": {
            "privacyStatus": "private"  # Можно изменить на "public" или "unlisted"
        }
    }
    media = MediaFileUpload(video_file, chunksize=-1, resumable=True)
    request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
    response = request.execute()
    end_time = datetime.now()
    upload_time = (end_time - start_time).total_seconds()
    file_size = os.path.getsize(video_file) / (1024 * 1024)  # Размер в МБ
    speed = file_size / upload_time if upload_time > 0 else 0
    msg = f"Видео {video_file} ({file_size:.2f} МБ) загружено на YouTube за {int(upload_time // 60)} мин {int(upload_time % 60)} сек, скорость: {speed:.2f} МБ/с"
    logging.info(msg)
    print(msg)
    return response["id"]

# Основная функция
def main(start_row=1, end_row=None, max_uploads=10, debug=False):
    # Настройка уровня логирования
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)

    setup_environment()  # Автоматическая настройка окружения
    setup_credentials()  # Настройка учетных данных

    uploaded_count = 0
    logging.info("Очистка старых файлов...")
    print("Очистка старых файлов...")
    for file in os.listdir():
        if file.endswith(".mp4"):
            os.remove(file)

    df = pd.read_excel(STREAMS_FILE)
    start_index = max(0, start_row - 1)
    end_index = end_row if end_row is not None else len(df)

    for index in range(start_index, end_index):
        if uploaded_count >= max_uploads:
            logging.info(f"Достигнут лимит загрузок: {max_uploads} видео.")
            print(f"Достигнут лимит загрузок: {max_uploads} видео.")
            break

        row = df.iloc[index]
        if pd.isna(row.iloc[1]):
            logging.info(f"Пропускаю строку {index + 1}: нет данных.")
            print(f"Пропускаю строку {index + 1}: нет данных.")
            continue
        logging.info(f"\nОбработка строки {index + 1}")
        print(f"\nОбработка строки {index + 1}")

        video_urls = row.iloc[1].split()
        video_files = []
        for url in video_urls:
            video_id = url.split("/")[-1]
            output_file = f"{video_id}.mp4"
            if not os.path.exists(output_file):
                download_twitch_video(url, output_file)
            video_files.append(output_file)

        name = str(row.iloc[2]) if pd.notna(row.iloc[2]) else ""
        description = str(row.iloc[3]) if pd.notna(row.iloc[3]) else ""
        tags = str(row.iloc[4]) if pd.notna(row.iloc[4]) else ""

        if len(video_files) > 1:
            first_id = video_files[0].split(".")[0]
            last_id = video_files[-1].split(".")[0]
            final_file = f"{first_id}-{last_id}.mp4"
            if not os.path.exists(final_file):
                concatenate_videos(video_files, final_file)
        else:
            final_file = video_files[0]

        total_duration = get_video_duration(final_file)
        logging.info(f"Длительность видео: {total_duration / 3600:.2f} часов")
        print(f"Длительность видео: {total_duration / 3600:.2f} часов")

        if total_duration <= 12 * 3600:
            if uploaded_count < max_uploads:
                upload_to_youtube(final_file, name, description, tags)
                uploaded_count += 1
        else:
            parts = split_single_video(final_file)
            for part_index, part_file in enumerate(parts):
                if uploaded_count >= max_uploads:
                    logging.info(f"Достигнут лимит загрузок: {max_uploads} видео.")
                    print(f"Достигнут лимит загрузок: {max_uploads} видео.")
                    break
                part_number = f"Часть {part_index + 1}"
                new_name = f"{name}. {part_number}" if not name.endswith((".", "!", "?")) else f"{name[:-1]} {part_number}{name[-1]}"
                upload_to_youtube(part_file, new_name, description, tags)
                uploaded_count += 1
                os.remove(part_file)

        logging.info("Удаляю временные файлы...")
        print("Удаляю временные файлы...")
        if len(video_files) > 1 and os.path.exists(final_file):
            os.remove(final_file)
        for video_file in video_files:
            if os.path.exists(video_file):
                os.remove(video_file)

    logging.info("Задача выполнена!")
    print("Задача выполнена!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Скрипт для загрузки видео на YouTube")
    parser.add_argument("--start", type=int, default=1, help="Начальная строка (с 1)")
    parser.add_argument("--end", type=int, help="Конечная строка")
    parser.add_argument("--max-uploads", type=int, default=10, help="Максимальное количество видео для загрузки (по умолчанию 10)")
    parser.add_argument("--debug", action="store_true", help="Включить подробное логирование")
    args = parser.parse_args()

    main(args.start, args.end, args.max_uploads, args.debug)