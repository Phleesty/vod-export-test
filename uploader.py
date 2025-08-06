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
import json
import math
import re
import urllib.request
from datetime import datetime

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow

CONFIG_FILE = "config.json"
TWITCH_DOWNLOADER_PATH = "./TwitchDownloaderCLI/TwitchDownloaderCLI"
FFMPEG_PATH = shutil.which("ffmpeg") or "/usr/bin/ffmpeg"
FFPROBE_PATH = shutil.which("ffprobe") or "/usr/bin/ffprobe"
STREAMS_FILE = "streams.xlsx"
CLIENT_SECRETS_FILE = "client_secret.json"
TOKEN_FILE = "token.json"
SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]
MAX_ALLOWED_DURATION = 11 * 3600 + 58 * 60  # 11:58:00

#############################
# Настройка и конфигурация  #
#############################

def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    return {}

def save_config(config):
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=2)

def setup_vkontakte_config():
    config = load_config()
    if not config.get("vk_token") or not config.get("vk_group_id") or not config.get("vk_album_id"):
        print("=== Настройка VK ===")
        config["vk_token"] = input("Введите VK access_token: ").strip()
        config["vk_group_id"] = int(input("Введите VK group_id: ").strip())
        config["vk_album_id"] = int(input("Введите VK album_id: ").strip())
        save_config(config)
    return config

def setup_youtube_credentials():
    if os.path.exists(CLIENT_SECRETS_FILE) and os.path.exists(TOKEN_FILE):
        choice = input("Продолжить с текущими настройками YouTube? (y/n): ").strip().lower()
        if choice == "y":
            return
    print("=== Настройка YouTube ===")
    client_secret = input("Вставьте JSON client_secret (или 'n' для пропуска): ")
    if client_secret.lower() != "n" and "{" in client_secret:
        with open(CLIENT_SECRETS_FILE, "w") as f:
            f.write(client_secret)

############################################################
# 1. TwitchDownloader: актуальная версия через GitHub API  #
############################################################

def get_latest_twitch_downloader_url():
    api_url = "https://api.github.com/repos/lay295/TwitchDownloader/releases/latest"
    response = requests.get(api_url, timeout=20)
    response.raise_for_status()
    release_data = response.json()
    for asset in release_data.get("assets", []):
        name = asset.get("name", "")
        if name.endswith("Linux-x64.zip") and name.startswith("TwitchDownloaderCLI-"):
            return asset["browser_download_url"]
    raise Exception("Не найден подходящий TwitchDownloaderCLI для Linux x64")

def ensure_twitch_downloader():
    if not os.path.exists(TWITCH_DOWNLOADER_PATH):
        print("Скачиваю TwitchDownloaderCLI (последняя версия)...")
        downloader_url = get_latest_twitch_downloader_url()
        print(f"URL последней версии: {downloader_url}")
        urllib.request.urlretrieve(downloader_url, "TwitchDownloaderCLI.zip")
        with zipfile.ZipFile("TwitchDownloaderCLI.zip", "r") as zip_ref:
            zip_ref.extractall("./TwitchDownloaderCLI")
        os.remove("TwitchDownloaderCLI.zip")
        print("TwitchDownloaderCLI загружен!")

def download_twitch_video(video_url, output_file):
    video_id = video_url.split("/")[-1]
    print(f"Скачиваю из Twitch: {video_url} → {output_file}")
    logging.info(f"Загрузка видео Twitch: {video_url}")
    command = [
        TWITCH_DOWNLOADER_PATH, "videodownload", "--id", video_id, "-o", output_file,
        "--threads", "20", "--temp-path", "temp"
    ]
    proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    pattern = re.compile(r"Downloading\s+(\d+)%")
    while True:
        line = proc.stdout.readline()
        if not line and proc.poll() is not None:
            break
        if line.strip():
            match = pattern.search(line)
            if match:
                percent = int(match.group(1))
                print(f"  [{output_file}] {percent}%", end="\r")
    proc.wait()
    print(f"  [{output_file}] 100%               ")
    logging.info(f"Файл {output_file} скачан.")

###########################
# Вспомогательные функции #
###########################

def get_video_duration(video_file):
    command = [
        FFPROBE_PATH, "-v", "error", "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1", video_file
    ]
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return float(result.stdout.strip())

def get_chapters(video_file):
    command = [FFPROBE_PATH, "-v", "quiet", "-print_format", "json", "-show_chapters", video_file]
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        data = json.loads(result.stdout)
    except Exception:
        data = {}
    return data.get("chapters", [])

def format_timestamp(seconds):
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)
    if hours > 0:
        return f"{hours:02}:{minutes:02}:{seconds:02}"
    else:
        return f"{minutes:02}:{seconds:02}"

def create_description_from_chapters(chapters):
    description = ""
    for chapter in chapters:
        start_time = float(chapter["start_time"])
        title = chapter["tags"].get("title", "Untitled")
        timestamp = format_timestamp(start_time)
        description += f"{timestamp} - {title}\n"
    return description

def create_concat_metadata(video_files):
    cumulative_duration = 0
    all_chapters = []
    for video_file in video_files:
        duration = get_video_duration(video_file)
        chapters = get_chapters(video_file)
        for chapter in chapters:
            adjusted = dict(chapter)
            adjusted["start_time"] = float(adjusted["start_time"]) + cumulative_duration
            adjusted["end_time"] = float(adjusted["end_time"]) + cumulative_duration
            all_chapters.append(adjusted)
        cumulative_duration += duration
    metadata_content = ";FFMETADATA1\n"
    for chapter in all_chapters:
        start = int(chapter["start_time"] * 1000)
        end = int(chapter["end_time"] * 1000)
        title = chapter["tags"].get("title", "Untitled")
        metadata_content += f"[CHAPTER]\nTIMEBASE=1/1000\nSTART={start}\nEND={end}\ntitle={title}\n"
    metadata_file = "concat_metadata.txt"
    with open(metadata_file, "w") as f:
        f.write(metadata_content)
    return metadata_file

def concatenate_videos(video_files, output_file, metadata_file=None):
    print("Объединяю файлы...")
    with open("concat_list.txt", "w") as f:
        for video_file in video_files:
            f.write(f"file '{video_file}'\n")
    command = [FFMPEG_PATH, "-f", "concat", "-safe", "0", "-i", "concat_list.txt"]
    if metadata_file:
        command += ["-i", metadata_file, "-map_metadata", "1"]
    command += ["-c", "copy", output_file]
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    os.remove("concat_list.txt")
    if metadata_file and os.path.exists(metadata_file):
        os.remove(metadata_file)
    print(f"Видео объединено в {output_file}")

def split_single_video(video_file, max_dur=MAX_ALLOWED_DURATION):
    duration = get_video_duration(video_file)
    if duration <= max_dur:
        return [video_file]
    parts = int(math.ceil(duration / max_dur))
    result_files = []
    for i in range(parts):
        start_time = int(i * max_dur)
        part_file = f"{video_file[:-4]}_part{i+1}.mp4"
        cmd = [
            FFMPEG_PATH, "-ss", str(start_time), "-i", video_file, "-t", str(max_dur), "-c", "copy", part_file
        ]
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        result_files.append(part_file)
    return result_files

#######################################
# 2. Загрузка видео в VK              #
#######################################

def upload_video_to_vk(token, group_id, video_path, album_id, name, description, privacy_view="all"):
    logging.info(f"Загрузка файла {video_path} в VK...")
    params = {
        "access_token": token,
        "v": "5.199",
        "group_id": abs(int(group_id)),
        "album_id": album_id,
        "name": name or "",
        "description": description or "",
        "privacy_view": privacy_view,
        "privacy_comment": "all"
    }
    response = requests.get("https://api.vk.com/method/video.save", params=params).json()
    if "error" in response:
        raise Exception(f"Ошибка VK API: {response['error']['error_msg']}")
    upload_url = response["response"]["upload_url"]
    with open(video_path, "rb") as video_file:
        from requests_toolbelt import MultipartEncoder
        encoder = MultipartEncoder(fields={"video_file": ("video_file", video_file, "video/mp4")})
        headers = {"Content-Type": encoder.content_type}
        upload_response = requests.post(upload_url, data=encoder, headers=headers)
        if not upload_response.ok:
            raise Exception(f"Ошибка при POST upload VK: {upload_response.text}")
    logging.info(f"{video_path} успешно загружен в VK.")
    return True

#######################################
# 3. Загрузка видео на YouTube        #
#######################################

def get_authenticated_youtube_service():
    if os.path.exists(TOKEN_FILE):
        credentials = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    else:
        flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
        credentials = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w") as token:
            token.write(credentials.to_json())
    return build("youtube", "v3", credentials=credentials)

def upload_to_youtube(video_file, title, description, tags):
    print(f"Загружаю {video_file} на YouTube...")
    logging.info(f"Загрузка {video_file} на YouTube")
    start_time = datetime.now()
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
    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()
    size_mb = os.path.getsize(video_file) / (1024 * 1024)
    print(f"  {video_file} ({size_mb:.2f} MB) загружено на YouTube за {int(elapsed//60)} мин {int(elapsed%60)} сек.")

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
    return f"{new_main_title} {date_part}"

#########################################
# 4. Основной процесс                   #
#########################################

def main(start_row=1, end_row=None, do_vk=True, do_youtube=True, max_uploads=99, debug=False):
    ensure_twitch_downloader()
    if do_vk:
        config = setup_vkontakte_config()
    if do_youtube:
        setup_youtube_credentials()
    logging.basicConfig(
        filename="upload_combined.log",
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    if not os.path.exists(STREAMS_FILE):
        print(f"Не найден {STREAMS_FILE}, проверьте наличие.")
        return

    print("Очистка временных .mp4 файлов перед запуском...")
    for f in os.listdir():
        if f.endswith(".mp4"):
            os.remove(f)

    df = pd.read_excel(STREAMS_FILE)
    start_index = max(0, start_row - 1)
    end_index = end_row if end_row is not None else len(df)
    uploaded_count = 0

    for index in range(start_index, end_index):
        row = df.iloc[index]
        # ---- Пропускаем пустые строки ----
        if pd.isna(row.iloc[1]):
            print(f"Строка {index+1}: нет Twitch-ссылки, пропускаю.")
            continue

        print(f"\n[{index+1}] Обрабатываю...")

        video_urls = row.iloc[1].split()
        video_files = []
        # ---- Скачивание (поочерёдно, чтобы видно было url/id) ----
        for url in video_urls:
            video_id = url.split("/")[-1] if "twitch.tv" in url else url
            output_file = f"{video_id}.mp4"
            print(f"-> Скачивание Twitch ID: {video_id}    ({url})")
            download_twitch_video(url, output_file)
            video_files.append(output_file)

        # ---- Объединяем если их несколько ----
        if len(video_files) > 1:
            metadata_file = create_concat_metadata(video_files)
            final_file = f"concatenated_{index+1}.mp4"
            concatenate_videos(video_files, final_file, metadata_file)
            for f in video_files:
                if os.path.exists(f):
                    os.remove(f)
            video_file = final_file
        else:
            video_file = video_files[0]

        # ---- Извлекаем описания, заголовки и теги ----
        name = str(row.iloc[2]) if pd.notna(row.iloc[2]) else ""
        chapters = get_chapters(video_file)
        tags = str(row.iloc[4]) if pd.notna(row.iloc[4]) else ""
        if chapters:
            description = create_description_from_chapters(chapters)
        else:
            description = str(row.iloc[3]) if pd.notna(row.iloc[3]) else ""

        # ---- 1. Сначала VK ----
        vk_ok = True
        if do_vk:
            try:
                print(f"-> Загрузка в VK: {video_file}")
                privacy = "2" if (len(row) > 7 and pd.notna(row.iloc[7]) and str(row.iloc[7]) == "1") else "all"
                upload_video_to_vk(
                    config["vk_token"], config["vk_group_id"], video_file,
                    config["vk_album_id"], name, description, privacy_view=privacy)
                print(f"-> VK: файл {video_file} успешно загружен.")
                logging.info(f"VK upload ok for {video_file}")
            except Exception as e:
                print(f"--!! Ошибка загрузки в VK: {e}")
                logging.error(f"Ошибка VK для {video_file}: {e}")
                vk_ok = False

        # ---- 2. YouTube, если надо, и VK успешен ----
        if do_youtube and vk_ok:
            to_upload = []
            duration = get_video_duration(video_file)
            # разделить на части если дольше лимита YouTube
            if duration > MAX_ALLOWED_DURATION:
                parts = split_single_video(video_file)
                to_upload.extend(parts)
            else:
                to_upload.append(video_file)

            for i, upload_file in enumerate(to_upload):
                if uploaded_count >= max_uploads:
                    print("Достигнут лимит YouTube загрузок (max-uploads).")
                    break
                y_chapters = get_chapters(upload_file)
                y_description = create_description_from_chapters(y_chapters) if y_chapters else description
                yt_title = add_part_to_title(name, i+1) if len(to_upload) > 1 else name
                try:
                    upload_to_youtube(upload_file, yt_title, y_description, tags)
                    print(f"-> YouTube: {upload_file} успешно загружен.")
                    logging.info(f"YouTube upload ok for {upload_file}")
                    uploaded_count += 1
                except Exception as e:
                    print(f"--!! Ошибка загрузки на YouTube: {e}")
                    logging.error(f"Ошибка YouTube для {upload_file}: {e}")

        # ---- Удаляем главный файл и части после загрузки на платформы ----
        try:
            files_for_cleanup = set(video_files + ([video_file] if video_file not in video_files else []))
            # если файл делился на части, тоже почистим
            for f in os.listdir():
                if (f.startswith(video_file[:-4]) and f.endswith(".mp4")) or f in files_for_cleanup:
                    try:
                        os.remove(f)
                    except:
                        pass
            print(f"Удалены все временные файлы для строки {index+1}.")
        except Exception as e:
            print(f"Ошибка при удалении файлов: {e}")

    print("\nВыполнено!\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Скрипт для групповой загрузки в VK и YouTube")
    parser.add_argument("--start", type=int, default=1, help="Начальная строка (с 1)")
    parser.add_argument("--end", type=int, help="Конечная строка (включительно)")
    parser.add_argument("--vk", action="store_true", help="Загружать только VK")
    parser.add_argument("--youtube", action="store_true", help="Загружать только YouTube")
    parser.add_argument("--max-uploads", type=int, default=99, help="Максимум файлов для YouTube за запуск")
    parser.add_argument("--debug", action="store_true", help="Подробный лог")
    args = parser.parse_args()
    # Флаги: если не выставлено ни одного, то обе платформы ("по умолчанию")
    do_vk = args.vk or (not args.vk and not args.youtube)
    do_youtube = args.youtube or (not args.vk and not args.youtube)
    main(args.start, args.end, do_vk, do_youtube, args.max_uploads, args.debug)
