import pandas as pd
import subprocess
import os
import requests
import json
import time
import threading
import shutil
import zipfile
import urllib.request
import re
import math
from datetime import datetime
from requests_toolbelt import MultipartEncoder
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn, SpinnerColumn
import argparse
import logging

# Константы
CONFIG_FILE = "config.json"
INSTALLED_FILE = ".installed"
TWITCH_DOWNLOADER_URL = "https://github.com/lay295/TwitchDownloader/releases/download/1.55.2/TwitchDownloaderCLI-1.55.2-Linux-x64.zip"
LBRYNET_URL = "https://github.com/lbryio/lbry-sdk/releases/latest/download/lbrynet-linux.zip"
CLIENT_SECRETS_FILE = "client_secret.json"
TOKEN_FILE = "token.json"
SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]
MAX_ALLOWED_DURATION = 11 * 3600 + 58 * 60  # 11 часов 58 минут
STREAMS_FILE = "vk.xlsx"

# Глобальная блокировка для вывода
print_lock = threading.Lock()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("upload_log.txt"), logging.StreamHandler()]
)

### Установка зависимостей
def install_dependencies():
    if not os.path.exists("./TwitchDownloaderCLI"):
        logging.info("Скачивание TwitchDownloaderCLI...")
        urllib.request.urlretrieve(TWITCH_DOWNLOADER_URL, "TwitchDownloaderCLI.zip")
        with zipfile.ZipFile("TwitchDownloaderCLI.zip", "r") as zip_ref:
            zip_ref.extractall("./TwitchDownloaderCLI")
        os.remove("TwitchDownloaderCLI.zip")
        subprocess.run(["chmod", "+x", "./TwitchDownloaderCLI/TwitchDownloaderCLI"], check=True)
    
    if not shutil.which("lbrynet"):
        logging.info("Скачивание lbrynet...")
        urllib.request.urlretrieve(LBRYNET_URL, "lbrynet.zip")
        with zipfile.ZipFile("lbrynet.zip", "r") as zip_ref:
            zip_ref.extractall(".")
        os.remove("lbrynet.zip")
        subprocess.run(["chmod", "+x", "lbrynet"], check=True)
        subprocess.run(["sudo", "mv", "lbrynet", "/usr/local/bin/"], check=True)
    
    if not shutil.which("ffmpeg"):
        logging.warning("FFmpeg не установлен. Установите его: 'sudo apt install ffmpeg'")
        with print_lock:
            print("FFmpeg не установлен. Установите его: 'sudo apt install ffmpeg'")
        exit(1)
    
    with open(INSTALLED_FILE, "w") as f:
        f.write("Dependencies installed")

### Настройка конфигурации
def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    return None

def save_config(config):
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=4)

def setup_config():
    config = {}
    config["vk_token"] = input("Введите ваш access токен VK: ")
    config["vk_group_id"] = int(input("Введите ID группы VK: "))
    config["vk_album_id"] = int(input("Введите ID альбома VK: "))
    config["wallet_path"] = input("Введите путь к файлу кошелька default_wallet (или оставьте пустым): ") or ""
    config["youtube_client_secret"] = input("Введите ваш client secrets file для YouTube (или 'n' для пропуска): ")
    if config["youtube_client_secret"].lower() != 'n':
        with open(CLIENT_SECRETS_FILE, "w") as f:
            f.write(config["youtube_client_secret"])
    config["youtube_token"] = input("Введите ваш токен файл для YouTube (или 'n' для пропуска): ")
    if config["youtube_token"].lower() != 'n':
        with open(TOKEN_FILE, "w") as f:
            f.write(config["youtube_token"])
    save_config(config)
    return config

### Работа с lbrynet
def start_lbrynet():
    logging.info("Запускаю lbrynet...")
    subprocess.Popen(["sudo", "lbrynet", "start"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(15)

def stop_lbrynet():
    logging.info("Останавливаю lbrynet...")
    subprocess.run(["lbrynet", "stop"])
    time.sleep(5)

def lbrynet_call(method, params=None):
    payload = {"jsonrpc": "2.0", "method": method, "params": params or {}, "id": int(time.time())}
    response = requests.post("http://localhost:5279", json=payload)
    return response.json()["result"]

### Скачивание и обработка видео
def download_twitch_video_rich(progress, task_id, video_url, output_file):
    start_time = datetime.now()
    video_id = video_url.split("/")[-1] if "twitch.tv" in video_url else video_url
    command = [
        "./TwitchDownloaderCLI/TwitchDownloaderCLI", "videodownload", "--id", video_id, "-o", output_file,
        "--threads", "20", "--temp-path", "temp"
    ]
    proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
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
    proc.wait()
    progress.update(task_id, completed=100)
    end_time = datetime.now()
    download_time = (end_time - start_time).total_seconds()
    file_size = os.path.getsize(output_file) / (1024 * 1024)
    speed = file_size / download_time if download_time > 0 else 0
    msg = f"Скачивание {output_file} ({file_size:.2f} МБ) завершено за {int(download_time // 60)} мин {int(download_time % 60)} сек, скорость: {speed:.2f} МБ/с"
    logging.info(msg)
    with print_lock:
        print(msg)

def get_video_duration(video_file):
    command = ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", video_file]
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return float(result.stdout.strip())

def concatenate_videos(video_files, output_file, progress, task_id):
    with open("concat_list.txt", "w") as f:
        for video_file in video_files:
            f.write(f"file '{video_file}'\n")
    command = ["ffmpeg", "-f", "concat", "-safe", "0", "-i", "concat_list.txt", "-c", "copy", output_file]
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    os.remove("concat_list.txt")
    progress.update(task_id, completed=100)

def split_single_video(video_file, progress, task_id):
    duration = get_video_duration(video_file)
    if duration <= MAX_ALLOWED_DURATION:
        return [video_file]
    
    parts = math.ceil(duration / MAX_ALLOWED_DURATION)
    part_files = []
    for i in range(parts):
        start_time = i * MAX_ALLOWED_DURATION
        part_file = f"{video_file[:-4]}_part{i+1}.mp4"
        command = ["ffmpeg", "-i", video_file, "-ss", str(start_time), "-t", str(MAX_ALLOWED_DURATION), "-c", "copy", part_file]
        subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        part_files.append(part_file)
        progress.update(task_id, advance=100 / parts)
    return part_files

### Загрузка на платформы
def upload_video_to_vk(token, group_id, video_path, album_id, name, description, privacy_view="all", progress=None, task_id=None):
    start_time = datetime.now()
    params = {
        "access_token": token, "v": "5.199", "group_id": abs(int(group_id)), "album_id": album_id,
        "name": name, "description": description, "privacy_view": privacy_view, "privacy_comment": "all"
    }
    response = requests.get("https://api.vk.com/method/video.save", params=params).json()
    upload_url = response["response"]["upload_url"]
    with open(video_path, "rb") as video_file:
        encoder = MultipartEncoder(fields={"video_file": ("video_file", video_file, "video/mp4")})
        headers = {"Content-Type": encoder.content_type}
        requests.post(upload_url, data=encoder, headers=headers)
    end_time = datetime.now()
    upload_time = (end_time - start_time).total_seconds()
    file_size = os.path.getsize(video_path) / (1024 * 1024)
    speed = file_size / upload_time if upload_time > 0 else 0
    msg = f"Загрузка на VK {video_path} ({file_size:.2f} МБ) завершена за {int(upload_time // 60)} мин {int(upload_time % 60)} сек, скорость: {speed:.2f} МБ/с"
    logging.info(msg)
    with print_lock:
        print(msg)
    if progress and task_id:
        progress.update(task_id, completed=100)

def upload_to_odysee(file_path, claim_name, channel_name, thumbnail_url, name, description, tags, visibility="public", progress=None, task_id=None):
    start_time = datetime.now()
    params = {
        "name": claim_name, "file_path": file_path, "title": name, "description": description,
        "channel_name": channel_name, "bid": "0.01", "tags": tags.split(", "), "languages": ["ru"],
        "license": "Public Domain", "thumbnail_url": thumbnail_url, "visibility": visibility
    }
    result = lbrynet_call("publish", params)
    claim_id = result["outputs"][0]["claim_id"]
    while True:
        claims = lbrynet_call("claim_search", {"claim_id": claim_id})
        if claims["items"] and claims["items"][0].get("confirmations", 0) > 0:
            break
        time.sleep(10)
    end_time = datetime.now()
    upload_time = (end_time - start_time).total_seconds()
    file_size = os.path.getsize(file_path) / (1024 * 1024)
    speed = file_size / upload_time if upload_time > 0 else 0
    msg = f"Загрузка на Odysee {file_path} ({file_size:.2f} МБ) завершена за {int(upload_time // 60)} мин {int(upload_time % 60)} сек, скорость: {speed:.2f} МБ/с"
    logging.info(msg)
    with print_lock:
        print(msg)
    if progress and task_id:
        progress.update(task_id, completed=100)

def get_authenticated_youtube_service():
    if os.path.exists(TOKEN_FILE):
        credentials = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    else:
        flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
        credentials = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w") as token:
            token.write(credentials.to_json())
    return build("youtube", "v3", credentials=credentials)

def upload_to_youtube(video_file, title, description, tags, progress=None, task_id=None):
    start_time = datetime.now()
    youtube = get_authenticated_youtube_service()
    body = {"snippet": {"title": title, "description": description, "tags": tags.split(", "), "categoryId": "22"}, "status": {"privacyStatus": "private"}}
    media = MediaFileUpload(video_file, chunksize=-1, resumable=True)
    youtube.videos().insert(part="snippet,status", body=body, media_body=media).execute()
    end_time = datetime.now()
    upload_time = (end_time - start_time).total_seconds()
    file_size = os.path.getsize(video_file) / (1024 * 1024)
    speed = file_size / upload_time if upload_time > 0 else 0
    msg = f"Загрузка на YouTube {video_file} ({file_size:.2f} МБ) завершена за {int(upload_time // 60)} мин {int(upload_time % 60)} сек, скорость: {speed:.2f} МБ/с"
    logging.info(msg)
    with print_lock:
        print(msg)
    if progress and task_id:
        progress.update(task_id, completed=100)

### Основная логика
def process_video_row(index, row, config, do_vk, do_odysee, do_youtube):
    video_urls = row.iloc[1].split()
    video_files = [f"video_{index + 1}_{i}.mp4" for i, url in enumerate(video_urls)]
    final_file = f"concatenated_{index + 1}.mp4"

    with Progress(transient=True) as progress:
        # Этап 1: Скачивание
        download_tasks = []
        for i, url in enumerate(video_urls):
            task = progress.add_task(f"Скачивание {video_files[i]}", total=100)
            download_tasks.append((task, url, video_files[i]))
        threads = []
        for task_id, url, output_file in download_tasks:
            thread = threading.Thread(target=download_twitch_video_rich, args=(progress, task_id, url, output_file))
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()

        # Этап 2: Проверка и объединение
        concat_task = progress.add_task(f"Объединение видео в {final_file}", total=100)
        if len(video_files) > 1:
            concatenate_videos(video_files, final_file, progress, concat_task)
        else:
            final_file = video_files[0]
            progress.update(concat_task, completed=100)

        # Этап 3: Разбиение для YouTube (если нужно)
        total_duration = get_video_duration(final_file)
        youtube_files = [final_file]
        if total_duration > MAX_ALLOWED_DURATION and do_youtube:
            split_task = progress.add_task(f"Разбиение {final_file} для YouTube", total=100)
            youtube_files = split_single_video(final_file, progress, split_task)
        else:
            progress.add_task("Разбиение для YouTube (не требуется)", total=100, completed=100)

        # Этап 4: Загрузка
        name = str(row.iloc[2]) if pd.notna(row.iloc[2]) else ""
        description = str(row.iloc[3]) if pd.notna(row.iloc[3]) else ""
        tags = str(row.iloc[4]) if pd.notna(row.iloc[4]) else ""
        claim_name = str(row.iloc[5]) if pd.notna(row.iloc[5]) else "default_claim_name"
        thumbnail_url = str(row.iloc[6]) if pd.notna(row.iloc[6]) else ""
        privacy_value = str(row.iloc[7]) if len(row) > 7 and pd.notna(row.iloc[7]) else ""
        vk_privacy = "2" if privacy_value == "1" else "all"
        odysee_visibility = "unlisted" if privacy_value == "1" else "public"

        upload_tasks = []
        threads = []
        if do_vk:
            vk_task = progress.add_task(f"Загрузка {final_file} на VK", total=100)
            thread = threading.Thread(target=upload_video_to_vk, args=(config["vk_token"], config["vk_group_id"], final_file, config["vk_album_id"], name, description, vk_privacy, progress, vk_task))
            threads.append(thread)
            thread.start()
        if do_odysee:
            odysee_task = progress.add_task(f"Загрузка {final_file} на Odysee", total=100)
            thread = threading.Thread(target=upload_to_odysee, args=(final_file, claim_name, "@unuasha", thumbnail_url, name, description, tags, odysee_visibility, progress, odysee_task))
            threads.append(thread)
            thread.start()
        if do_youtube:
            for i, yt_file in enumerate(youtube_files):
                yt_title = f"{name} Часть {i+1}" if len(youtube_files) > 1 else name
                yt_task = progress.add_task(f"Загрузка {yt_file} на YouTube", total=100)
                thread = threading.Thread(target=upload_to_youtube, args=(yt_file, yt_title, description, tags, progress, yt_task))
                threads.append(thread)
                thread.start()
        for thread in threads:
            thread.join()

    # Очистка
    for file in video_files + youtube_files + ([final_file] if final_file not in video_files else []):
        if os.path.exists(file):
            os.remove(file)

def main(start_row=1, end_row=None, max_uploads=10, debug=False, do_vk=True, do_odysee=True, do_youtube=True):
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)

    if not os.path.exists(INSTALLED_FILE):
        install_dependencies()

    config = load_config()
    if config and input("Продолжить с сохраненными настройками? (y/n): ").lower() == "y":
        pass
    else:
        config = setup_config()

    if do_odysee:
        start_lbrynet()

    df = pd.read_excel(STREAMS_FILE)
    start_index = max(0, start_row - 1)
    end_index = min(end_row if end_row is not None else len(df), start_index + max_uploads)

    for index in range(start_index, end_index):
        row = df.iloc[index]
        if pd.isna(row.iloc[1]):
            logging.info(f"Пропускаю строку {index + 1}: нет данных.")
            continue
        logging.info(f"\nОбработка строки {index + 1}")
        process_video_row(index, row, config, do_vk, do_odysee, do_youtube)

    if do_odysee:
        stop_lbrynet()
    logging.info("Задача успешно выполнена!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Скрипт для загрузки видео на VK, Odysee и YouTube")
    parser.add_argument("--start", type=int, default=1, help="Начальная строка (с 1)")
    parser.add_argument("--end", type=int, help="Конечная строка")
    parser.add_argument("--max-uploads", type=int, default=10, help="Максимальное количество видео")
    parser.add_argument("--debug", action="store_true", help="Включить отладку")
    parser.add_argument("--vk", action="store_true", help="Загружать на VK")
    parser.add_argument("--odysee", action="store_true", help="Загружать на Odysee")
    parser.add_argument("--youtube", action="store_true", help="Загружать на YouTube")
    args = parser.parse_args()

    do_vk = args.vk or not (args.vk or args.odysee or args.youtube)
    do_odysee = args.odysee or not (args.vk or args.odysee or args.youtube)
    do_youtube = args.youtube or not (args.vk or args.odysee or args.youtube)
    main(args.start, args.end, args.max_uploads, args.debug, do_vk, do_odysee, do_youtube)