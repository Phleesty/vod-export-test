import pandas as pd
import subprocess
import os
import argparse
import logging
import requests
import zipfile
import shutil
import threading
import time
import re
from datetime import datetime
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn

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

# Глобальная блокировка для безопасного вывода (для сообщений, не связанных с прогресс-барами)
print_lock = threading.Lock()
def safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)

# Функция для автоматической настройки окружения
def setup_environment():
    if not shutil.which("ffmpeg"):
        logging.warning("FFmpeg не установлен. Установите его командой: 'sudo apt install ffmpeg'")
        safe_print("FFmpeg не установлен. Установите его командой: 'sudo apt install ffmpeg'")
        exit(1)
    if not os.path.exists("./TwitchDownloaderCLI"):
        logging.info("Скачиваю TwitchDownloaderCLI...")
        safe_print("Скачиваю TwitchDownloaderCLI...")
        url = "https://github.com/lay295/TwitchDownloader/releases/download/1.55.2/TwitchDownloaderCLI-1.55.2-Linux-x64.zip"
        response = requests.get(url)
        with open("TwitchDownloaderCLI.zip", "wb") as f:
            f.write(response.content)
        with zipfile.ZipFile("TwitchDownloaderCLI.zip", "r") as zip_ref:
            zip_ref.extractall("./TwitchDownloaderCLI")
        os.remove("TwitchDownloaderCLI.zip")
        logging.info("TwitchDownloaderCLI успешно установлен.")
        safe_print("TwitchDownloaderCLI успешно установлен.")

# Функция для настройки учетных данных
def setup_credentials():
    if os.path.exists(CLIENT_SECRETS_FILE) and os.path.exists(TOKEN_FILE):
        choice = input("Хотите продолжить с сохраненными настройками? (y/n): ").strip().lower()
        if choice == 'y':
            return
        else:
            logging.info("Настройка новых учетных данных...")
            safe_print("Настройка новых учетных данных...")

    client_secret = input("Введите ваш client secrets file (или 'n' для пропуска): ").strip()
    if client_secret.lower() != 'n':
        with open(CLIENT_SECRETS_FILE, "w") as f:
            f.write(client_secret)
        logging.info(f"{CLIENT_SECRETS_FILE} успешно сохранен.")
        safe_print(f"{CLIENT_SECRETS_FILE} успешно сохранен.")

    token = input("Введите ваш токен файл (или 'n' для пропуска): ").strip()
    if token.lower() != 'n':
        with open(TOKEN_FILE, "w") as f:
            f.write(token)
        logging.info(f"{TOKEN_FILE} успешно сохранен.")
        safe_print(f"{TOKEN_FILE} успешно сохранен.")

# Функция для скачивания видео с Twitch с обновляемым прогресс-баром через rich
def download_twitch_video_rich(progress, task_id, video_url, output_file):
    start_time = datetime.now()
    video_id = video_url.split("/")[-1] if "twitch.tv" in video_url else video_url
    command = [
        TWITCH_DOWNLOADER_PATH, "videodownload", "--id", video_id, "-o", output_file,
        "--threads", "20", "--temp-path", "temp"
    ]
    logging.debug(f"Выполняю команду: {' '.join(command)}")
    try:
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    except Exception as e:
        logging.error(f"Ошибка запуска процесса: {e}")
        progress.update(task_id, description=f"{output_file} ERROR")
        return

    # Регулярное выражение для поиска процентов загрузки
    pattern = re.compile(r"Downloading\s+(\d+)%")
    while True:
        line = proc.stdout.readline()
        if not line and proc.poll() is not None:
            break
        if line:
            match = pattern.search(line)
            if match:
                percent = int(match.group(1))
                progress.update(task_id, completed=percent)
    retcode = proc.wait()
    if retcode != 0:
        raise subprocess.CalledProcessError(retcode, command)
    # Обновляем до 100% и удаляем задачу, чтобы прогресс-бар исчез
    progress.update(task_id, completed=100)
    progress.remove_task(task_id)
    end_time = datetime.now()
    download_time = (end_time - start_time).total_seconds()
    file_size = os.path.getsize(output_file) / (1024 * 1024)  # Размер в МБ
    speed = file_size / download_time if download_time > 0 else 0
    msg = (f"Файл {output_file} ({file_size:.2f} МБ) скачан за "
           f"{int(download_time // 60)} мин {int(download_time % 60)} сек, скорость: {speed:.2f} МБ/с")
    logging.info(msg)
    safe_print(msg)

# Функция для получения длительности видео
def get_video_duration(video_file):
    command = [
        FFPROBE_PATH, "-v", "error", "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1", video_file
    ]
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return float(result.stdout.strip())

# Функция для объединения видео
def concatenate_videos(video_files, output_file):
    logging.info("Объединяю видео...")
    safe_print("Объединяю видео...")
    with open("concat_list.txt", "w") as f:
        for video_file in video_files:
            f.write(f"file '{video_file}'\n")
    command = [FFMPEG_PATH, "-f", "concat", "-safe", "0", "-i", "concat_list.txt", "-c", "copy", output_file]
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    os.remove("concat_list.txt")
    logging.info(f"Видео объединено в {output_file}")
    safe_print(f"Видео объединено в {output_file}")

# Функция для разделения видео на части
def split_single_video(video_file, max_duration=12*3600, safe_duration=10*3600):
    duration = get_video_duration(video_file)
    parts = []
    start_time = 0
    part_num = 1

    while start_time < duration:
        part_duration = min(safe_duration, duration - start_time)
        part_file = f"{video_file[:-4]}_part{part_num}.mp4"
        command = [
            FFMPEG_PATH, "-i", video_file, "-ss", str(start_time),
            "-t", str(part_duration), "-c", "copy", part_file
        ]
        logging.info(f"Разделяю {video_file} на часть {part_num}...")
        safe_print(f"Разделяю {video_file} на часть {part_num}...")
        subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        parts.append(part_file)
        start_time += part_duration
        part_num += 1
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

# Функция для загрузки видео на YouTube (в потоке)
def upload_to_youtube(video_file, title, description, tags):
    logging.info(f"Загружаю {video_file} на YouTube как '{title}'...")
    safe_print(f"Загружаю {video_file} на YouTube как '{title}'...")
    start_time = datetime.now()
    youtube = get_authenticated_youtube_service()
    body = {
        "snippet": {
            "title": title,
            "description": description,
            "tags": tags.split(", "),
            "categoryId": "22"
        },
        "status": {
            "privacyStatus": "private"
        }
    }
    media = MediaFileUpload(video_file, chunksize=-1, resumable=True)
    request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
    response = request.execute()
    end_time = datetime.now()
    upload_time = (end_time - start_time).total_seconds()
    file_size = os.path.getsize(video_file) / (1024 * 1024)
    speed = file_size / upload_time if upload_time > 0 else 0
    msg = (f"Видео {video_file} ({file_size:.2f} МБ) загружено за "
           f"{int(upload_time // 60)} мин {int(upload_time % 60)} сек, скорость: {speed:.2f} МБ/с")
    logging.info(msg)
    safe_print(msg)

# Умная группировка видео
def smart_group_and_concatenate(video_files, max_duration=12*3600):
    durations = [get_video_duration(vf) for vf in video_files]
    groups = []
    current_group = []
    current_duration = 0

    for duration, video_file in zip(durations, video_files):
        if current_duration + duration <= max_duration:
            current_group.append(video_file)
            current_duration += duration
        else:
            if current_group:
                groups.append(current_group)
            current_group = [video_file]
            current_duration = duration

    if current_group:
        groups.append(current_group)

    final_files = []
    for i, group in enumerate(groups):
        if len(group) == 1:
            final_files.append(group[0])
        else:
            output_file = f"group_{i}.mp4"
            concatenate_videos(group, output_file)
            final_files.append(output_file)

    return final_files

# Функция для формирования названия с номером части
def add_part_to_title(title, part_number):
    last_open_paren = title.rfind("(")
    if last_open_paren == -1:
        return f"{title}. Часть {part_number}"
    
    date_part = title[last_open_paren:]
    main_title = title[:last_open_paren].strip()
    
    if main_title and main_title[-1] in ".!?":
        new_main_title = f"{main_title} Часть {part_number}"
    else:
        new_main_title = f"{main_title}. Часть {part_number}"
    
    new_title = f"{new_main_title} {date_part}"
    return new_title

# Основная функция
def main(start_row=1, end_row=None, max_uploads=10, debug=False):
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)

    setup_environment()
    setup_credentials()

    uploaded_count = 0
    logging.info("Очистка старых файлов...")
    safe_print("Очистка старых файлов...")
    for file in os.listdir():
        if file.endswith(".mp4"):
            os.remove(file)

    df = pd.read_excel(STREAMS_FILE)
    start_index = max(0, start_row - 1)
    end_index = end_row if end_row is not None else len(df)

    # Используем rich.Progress для отображения прогресс-баров скачивания
    progress_columns = [
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TextColumn("{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        TimeRemainingColumn()
    ]
    with Progress(*progress_columns, transient=True) as progress:
        for index in range(start_index, end_index):
            if uploaded_count >= max_uploads:
                logging.info(f"Достигнут лимит загрузок: {max_uploads} видео.")
                safe_print(f"Достигнут лимит загрузок: {max_uploads} видео.")
                break

            row = df.iloc[index]
            if pd.isna(row.iloc[1]):
                logging.info(f"Пропускаю строку {index + 1}: нет данных.")
                safe_print(f"Пропускаю строку {index + 1}: нет данных.")
                continue
            logging.info(f"\nОбработка строки {index + 1}")
            safe_print(f"\nОбработка строки {index + 1}")

            video_urls = row.iloc[1].split()
            video_files = []
            download_threads = []

            # Для каждого видео создаём задачу в progress и запускаем поток
            for url in video_urls:
                video_id = url.split("/")[-1]
                output_file = f"{video_id}.mp4"
                if not os.path.exists(output_file):
                    task_id = progress.add_task(f"[{output_file}]", total=100)
                    thread = threading.Thread(
                        target=download_twitch_video_rich,
                        args=(progress, task_id, url, output_file)
                    )
                    download_threads.append(thread)
                    thread.start()

            # Ждём завершения всех потоков скачивания
            for thread in download_threads:
                thread.join()

            video_files = [f"{url.split('/')[-1]}.mp4" for url in video_urls]

            name = str(row.iloc[2]) if pd.notna(row.iloc[2]) else ""
            description = str(row.iloc[3]) if pd.notna(row.iloc[3]) else ""
            tags = str(row.iloc[4]) if pd.notna(row.iloc[4]) else ""

            # Умная группировка
            grouped_files = smart_group_and_concatenate(video_files)

            for final_file in grouped_files:
                total_duration = get_video_duration(final_file)
                logging.info(f"Длительность видео {final_file}: {total_duration / 3600:.2f} часов")
                safe_print(f"Длительность видео {final_file}: {total_duration / 3600:.2f} часов")

                if total_duration <= 12 * 3600:
                    if uploaded_count < max_uploads:
                        upload_to_youtube(final_file, name, description, tags)
                        uploaded_count += 1
                else:
                    parts = split_single_video(final_file)
                    upload_threads = []
                    for part_index, part_file in enumerate(parts):
                        if uploaded_count >= max_uploads:
                            break
                        part_number = part_index + 1
                        new_name = add_part_to_title(name, part_number)
                        thread = threading.Thread(
                            target=upload_to_youtube,
                            args=(part_file, new_name, description, tags)
                        )
                        upload_threads.append(thread)
                        thread.start()
                        time.sleep(10)  # Задержка 10 секунд между загрузками
                        uploaded_count += 1

                    for thread in upload_threads:
                        thread.join()

                    for part_file in parts:
                        os.remove(part_file)

            logging.info("Удаляю временные файлы...")
            safe_print("Удаляю временные файлы...")
            for video_file in video_files:
                if os.path.exists(video_file):
                    os.remove(video_file)
            for grouped_file in grouped_files:
                if os.path.exists(grouped_file) and grouped_file not in video_files:
                    os.remove(grouped_file)

    logging.info("Задача выполнена!")
    safe_print("Задача выполнена!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Скрипт для загрузки видео на YouTube")
    parser.add_argument("--start", type=int, default=1, help="Начальная строка (с 1)")
    parser.add_argument("--end", type=int, help="Конечная строка")
    parser.add_argument("--max-uploads", type=int, default=10, help="Максимальное количество видео для загрузки")
    parser.add_argument("--debug", action="store_true", help="Включить подробное логирование")
    args = parser.parse_args()

    main(args.start, args.end, args.max_uploads, args.debug)
