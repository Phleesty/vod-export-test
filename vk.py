import pandas as pd
import subprocess
import os
import requests
import json
import time
import threading
import shutil
from datetime import datetime
import argparse
import logging
import zipfile
import urllib.request
import sys
from requests_toolbelt import MultipartEncoder

# Константы остаются без изменений
CONFIG_FILE = "config.json"
INSTALLED_FILE = ".installed"
# TWITCH_DOWNLOADER_URL = "https://github.com/lay295/TwitchDownloader/releases/download/1.55.2/TwitchDownloaderCLI-1.55.2-Linux-x64.zip"
LBRYNET_URL = "https://github.com/lbryio/lbry-sdk/releases/latest/download/lbrynet-linux.zip"

def get_latest_twitch_downloader_url():
    """Получает ссылку на последнюю версию TwitchDownloaderCLI для Linux x64 с GitHub."""
    api_url = "https://api.github.com/repos/lay295/TwitchDownloader/releases/latest"
    response = requests.get(api_url)
    response.raise_for_status()
    release_data = response.json()
    for asset in release_data.get("assets", []):
        name = asset.get("name", "")
        if name.endswith("Linux-x64.zip") and name.startswith("TwitchDownloaderCLI-"):
            return asset["browser_download_url"]
    raise Exception("Не удалось найти подходящий TwitchDownloaderCLI для Linux x64")

def install_dependencies():
    logging.info("Скачивание TwitchDownloaderCLI...")
    twitch_url = get_latest_twitch_downloader_url()
    urllib.request.urlretrieve(twitch_url, "TwitchDownloaderCLI.zip")
    with zipfile.ZipFile("TwitchDownloaderCLI.zip", "r") as zip_ref:
        zip_ref.extractall("TwitchDownloaderCLI")
    os.remove("TwitchDownloaderCLI.zip")
    logging.info("Скачивание lbrynet...")
    urllib.request.urlretrieve(LBRYNET_URL, "lbrynet.zip")
    with zipfile.ZipFile("lbrynet.zip", "r") as zip_ref:
        zip_ref.extractall(".")
    os.remove("lbrynet.zip")
    subprocess.run(["chmod", "+x", "lbrynet"], check=True)
    subprocess.run(["sudo", "mv", "lbrynet", "/usr/local/bin/"], check=True)
    with open(INSTALLED_FILE, "w") as f:
        f.write("Dependencies installed")

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
    
    print("Загрузите файл `vk.xlsx` в текущую папку командой:")
    print("scp путь_к_файлу user@сервер:~/twitch-upload-script/vk.xlsx")
    while True:
        choice = input("Нажмите y, если загрузили, n — пропустить: ").lower()
        if choice == "y" and os.path.exists("streams.xlsx"):
            config["streams_file"] = "vk.xlsx"
            break
        elif choice == "n":
            config["streams_file"] = input("Введите путь к файлу vk.xlsx вручную: ")
            break
    
    print("Загрузите файл `default_wallet` в текущую папку командой:")
    print("scp путь_к_файлу user@сервер:~/twitch-upload-script/default_wallet")
    while True:
        choice = input("Нажмите y, если загрузили, n — пропустить: ").lower()
        if choice == "y" and os.path.exists("default_wallet"):
            config["wallet_path"] = "default_wallet"
            break
        elif choice == "n":
            config["wallet_path"] = input("Введите путь к файлу кошелька default_wallet (или оставьте пустым): ") or ""
            break
    
    save_config(config)
    return config

def start_lbrynet():
    logging.info("Запускаю lbrynet...")
    subprocess.Popen(["sudo", "lbrynet", "start"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    required_components = ["wallet", "file_manager", "blob_manager", "database"]
    while True:
        try:
            status = lbrynet_call("status")
            logging.info("Ждем включения компонентов lbrynet")
            component_status = status.get("startup_status", {})
            if all(component_status.get(component, False) for component in required_components):
                logging.info("Все компоненты lbrynet готовы к работе.")
                break
            else:
                logging.info(f"Ожидаю запуска компонентов: {component_status}")
        except Exception as e:
            logging.error(f"Ошибка при проверке статуса: {e}")
        time.sleep(15)

def stop_lbrynet():
    logging.info("Останавливаю lbrynet...")
    subprocess.run(["lbrynet", "stop"])
    time.sleep(5)

def download_twitch_video(video_url, output_file, progress_dict, lock, thread_id):
    start_time = datetime.now()
    video_id = video_url.split("/")[-1] if "twitch.tv" in video_url else video_url
    command = [
        "TwitchDownloaderCLI/TwitchDownloaderCLI", "videodownload", "--id", video_id, "-o", output_file,
        "--threads", "20", "--temp-path", "temp"
    ]
    logging.info(f"Скачиваю видео с ID {video_id} в {output_file}...")
    
    # Запускаем процесс и перенаправляем вывод в PIPE
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    
    # Читаем вывод построчно в реальном времени
    while process.poll() is None:
        line = process.stdout.readline().strip()
        if line and "may not have enough free space" not in line:  # Фильтруем ненужные строки
            with lock:
                progress_dict[thread_id] = line  # Сохраняем последнюю строку прогресса для этого потока
    
    # После завершения процесса фиксируем итоговую информацию
    end_time = datetime.now()
    download_time = (end_time - start_time).total_seconds()
    file_size = os.path.getsize(output_file) / (1024 * 1024)
    speed = file_size / download_time if download_time > 0 else 0
    msg = (f"Файл {output_file} ({file_size:.2f} МБ) скачан за "
           f"{int(download_time // 60)} мин {int(download_time % 60)} сек, скорость: {speed:.2f} МБ/с")
    with lock:
        progress_dict[thread_id] = msg  # Сохраняем финальное сообщение
    logging.info(msg)

def display_progress(progress_dict, lock, stop_event):
    """Отображает прогресс для всех потоков в консоли."""
    while not stop_event.is_set():
        with lock:
            print("\033[2J\033[H")  # Очищаем консоль и возвращаем курсор в начало
            for thread_id, progress in progress_dict.items():
                if progress:
                    print(f"[Thread {thread_id}] {progress}")
            sys.stdout.flush()
        time.sleep(0.1)  # Обновляем каждые 0.1 секунды

def concatenate_videos(video_files, output_file):
    logging.info("Объединение файлов...")
    with open("concat_list.txt", "w") as f:
        for video_file in video_files:
            f.write(f"file '{video_file}'\n")
    command = ["ffmpeg", "-f", "concat", "-safe", "0", "-i", "concat_list.txt", "-c", "copy", output_file]
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    os.remove("concat_list.txt")
    logging.info("Объединение файлов завершилось успешно.")

def upload_video_to_vk(token, group_id, video_path, album_id, name, description, privacy_view="all"):
    start_time = datetime.now()
    logging.info(f"Начинаю загрузку {video_path} в VK видео с приватностью {privacy_view}...")
    params = {
        "access_token": token,
        "v": "5.199",
        "group_id": abs(int(group_id)),
        "album_id": album_id,
        "name": str(name) if name else "",
        "description": str(description) if description else "",
        "privacy_view": privacy_view,
        "privacy_comment": "all"
    }
    response = requests.get("https://api.vk.ru/method/video.save", params=params).json()
    if "error" in response:
        raise Exception(f"Ошибка VK API: {response['error']['error_msg']}")
    upload_url = response["response"]["upload_url"]
    
    with open(video_path, "rb") as video_file:
        encoder = MultipartEncoder(fields={"video_file": ("video_file", video_file, "video/mp4")})
        headers = {"Content-Type": encoder.content_type}
        upload_response = requests.post(upload_url, data=encoder, headers=headers)
    
    end_time = datetime.now()
    upload_time = (end_time - start_time).total_seconds()
    file_size = os.path.getsize(video_path) / (1024 * 1024)
    speed = file_size / upload_time if upload_time > 0 else 0
    logging.info(f"Файл {video_path} ({file_size:.2f} МБ) загружен в VK за {int(upload_time // 60)} мин {int(upload_time % 60)} сек, скорость: {speed:.2f} МБ/с")
    return "video_id" in upload_response.json()

def lbrynet_call(method, params=None):
    payload = {"jsonrpc": "2.0", "method": method, "params": params or {}, "id": int(time.time())}
    response = requests.post("http://localhost:5279", json=payload)
    return response.json()["result"]

def wait_for_publish_completion(claim_id, debug=False):
    logging.info("Жду завершения публикации в Odysee...")
    while True:
        claims = lbrynet_call("claim_search", {"claim_id": claim_id})
        if debug:
            logging.debug(f"DEBUG: Ответ claim_search: {claims}")
        if claims["items"] and claims["items"][0].get("confirmations", 0) > 0:
            logging.info(f"Публикация {claim_id} успешно завершена в Odysee!")
            return True
        time.sleep(10)

def wait_for_file_upload_completion(claim_id, debug=False):
    logging.info("Жду завершения загрузки и отражения blob-файлов в Odysee...")
    time.sleep(5)
    start_time = time.time()
    max_wait_time = 9999
    while time.time() - start_time < max_wait_time:
        try:
            file_list = lbrynet_call("file_list", {"claim_id": claim_id})
            if debug:
                logging.debug(f"DEBUG: Ответ file_list: {file_list}")
            if file_list and "items" in file_list and len(file_list["items"]) > 0:
                file_status = file_list["items"][0]
                status = file_status.get("status", "unknown")
                blobs_remaining = file_status.get("blobs_remaining", -1)
                is_fully_reflected = file_status.get("is_fully_reflected", False)
                if debug:
                    logging.debug(f"DEBUG: Статус: {status}, is_fully_reflected: {is_fully_reflected}, blobs_remaining: {blobs_remaining}")
                if status == "finished" and blobs_remaining == 0 and is_fully_reflected:
                    logging.info(f"Blob-файлы для {claim_id} успешно загружены и отражены!")
                    return True
        except Exception as e:
            if debug:
                logging.debug(f"DEBUG: Ошибка при проверке статуса: {e}")
        time.sleep(10)
    logging.warning(f"Предупреждение: Время ожидания загрузки и отражения blob-файлов для {claim_id} истекло (30 минут).")
    return False

def upload_to_odysee(file_path, claim_name, channel_name, thumbnail_url, name, description, tags, visibility="public", debug=False):
    start_time = datetime.now()
    logging.info(f"Начинаю загрузку {file_path} на Odysee ({claim_name}) с видимостью {visibility}...")
    params = {
        "name": str(claim_name) if claim_name else "default_claim_name",
        "file_path": str(file_path),
        "title": str(name) if name else "",
        "description": str(description) if description else "",
        "channel_name": str(channel_name),
        "bid": "0.01",
        "tags": [str(tag) for tag in tags.split(", ")] if tags else [],
        "languages": ["ru"],
        "license": "Public Domain",
        "thumbnail_url": str(thumbnail_url) if thumbnail_url else "",
        "visibility": visibility
    }
    try:
        result = lbrynet_call("publish", params)
        if debug:
            logging.debug(f"DEBUG: Ответ publish: {result}")
        if "outputs" in result and len(result["outputs"]) > 0 and "claim_id" in result["outputs"][0]:
            claim_id = result["outputs"][0]["claim_id"]
            if wait_for_publish_completion(claim_id, debug) and wait_for_file_upload_completion(claim_id, debug):
                end_time = datetime.now()
                upload_time = (end_time - start_time).total_seconds()
                file_size = os.path.getsize(file_path) / (1024 * 1024)
                speed = file_size / upload_time if upload_time > 0 else 0
                logging.info(f"Файл {file_path} ({file_size:.2f} МБ) загружен в Odysee за {int(upload_time // 60)} мин {int(upload_time % 60)} сек, скорость: {speed:.2f} МБ/с")
                return claim_id
    except Exception as e:
        logging.error(f"Ошибка при публикации на Odysee: {e}")
    logging.error("Ошибка: Не удалось получить claim_id или завершить загрузку")
    return None

# Функция для извлечения глав из видео
def get_chapters(video_file):
    command = [
        "ffprobe", "-v", "quiet", "-print_format", "json", "-show_chapters", video_file
    ]
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    data = json.loads(result.stdout)
    return data.get("chapters", [])

# Функция для форматирования времени
def format_timestamp(seconds):
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    else:
        return f"{minutes:02d}:{seconds:02d}"

# Функция для создания описания из глав
def create_description_from_chapters(chapters):
    description = ""
    for chapter in chapters:
        start_time = float(chapter["start_time"])
        title = chapter["tags"].get("title", "Untitled")
        timestamp = format_timestamp(start_time)
        description += f"{timestamp} - {title}\n"
    return description

def main(start_row=1, end_row=None, do_vk_upload=True, do_odysee_upload=True, debug=False):
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("upload_log.txt"),
            logging.StreamHandler()
        ]
    )
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)

    if not os.path.exists(INSTALLED_FILE):
        install_dependencies()

    config = load_config()
    if config:
        if input("Продолжить с сохраненными настройками? (y/n): ").lower() != "y":
            config = setup_config()
    else:
        config = setup_config()

    STREAMS_FILE = config["streams_file"]
    VK_TOKEN = config["vk_token"]
    VK_GROUP_ID = config["vk_group_id"]
    VK_ALBUM_ID = config["vk_album_id"]
    BLOBFILES_PATH = os.path.expanduser("~/.local/share/lbry/lbrynet/blobfiles")

    if config["wallet_path"]:
        wallet_dest = os.path.expanduser("~/.local/share/lbry/lbrynet/wallets/default_wallet")
        os.makedirs(os.path.dirname(wallet_dest), exist_ok=True)
        shutil.copy(config["wallet_path"], wallet_dest)
        logging.info("Кошелек скопирован.")

    logging.info("Очистка старых видеофайлов и blob-файлов перед запуском...")
    for file in os.listdir():
        if file.endswith(".mp4"):
            os.remove(file)
    if os.path.exists(BLOBFILES_PATH):
        shutil.rmtree(BLOBFILES_PATH, ignore_errors=True)

    if do_odysee_upload:
        start_lbrynet()

    df = pd.read_excel(STREAMS_FILE)
    start_index = max(0, start_row - 1)
    end_index = end_row if end_row is not None else len(df)

    for index in range(start_index, end_index):
        row = df.iloc[index]
        if pd.isna(row.iloc[1]):
            logging.info(f"Пропускаю строку {index + 1}: нет данных для загрузки.")
            continue
        logging.info(f"\nОбработка строки {index + 1}")
        
        if pd.notna(row.iloc[0]):
            logging.info(f"Найдена запись в ячейке A: {row.iloc[0]}")
            if input("Продолжить? (y/n): ").lower() != "y":
                break

        video_urls = row.iloc[1].split()
        video_files = [f"video_{index + 1}_{i}.mp4" for i, url in enumerate(video_urls)]
        
        # Инициализируем словарь для хранения прогресса и блокировку
        progress_dict = {i: "" for i in range(len(video_urls))}
        lock = threading.Lock()
        stop_event = threading.Event()

        # Запускаем поток для отображения прогресса
        display_thread = threading.Thread(target=display_progress, args=(progress_dict, lock, stop_event))
        display_thread.start()

        # Запускаем загрузку видео в потоках
        threads = []
        for i, url in enumerate(video_urls):
            thread = threading.Thread(target=download_twitch_video, args=(url, video_files[i], progress_dict, lock, i))
            threads.append(thread)
            thread.start()
        
        # Ждем завершения всех потоков загрузки
        for thread in threads:
            thread.join()
        
        # Останавливаем отображение прогресса
        stop_event.set()
        display_thread.join()

        # Очищаем консоль после завершения загрузки
        print("\033[2J\033[H")
        for i in range(len(video_urls)):
            print(f"[Thread {i}] {progress_dict[i]}")

        if len(video_files) > 1:
            final_file = f"concatenated_{index + 1}.mp4"
            concatenate_videos(video_files, final_file)
            for temp_file in video_files:
                os.remove(temp_file)
            video_file = final_file
        else:
            video_file = video_files[0]

        # Установка параметров видео
        name = str(row.iloc[2]) if pd.notna(row.iloc[2]) else ""
        chapters = get_chapters(video_file)
        if chapters:
            description = create_description_from_chapters(chapters)
        else:
            description = str(row.iloc[3]) if pd.notna(row.iloc[3]) else ""
        tags = str(row.iloc[4]) if pd.notna(row.iloc[4]) else ""
        claim_name = str(row.iloc[5]) if pd.notna(row.iloc[5]) else "default_claim_name"
        thumbnail_url = str(row.iloc[6]) if pd.notna(row.iloc[6]) else ""
        privacy_value = str(row.iloc[7]) if len(row) > 7 and pd.notna(row.iloc[7]) else ""

        vk_privacy_view = "2" if privacy_value == "1" else "all"
        odysee_visibility = "unlisted" if privacy_value == "1" else "public"

        vk_success = [False]
        odysee_success = [False]

        def vk_upload():
            try:
                vk_success[0] = upload_video_to_vk(VK_TOKEN, VK_GROUP_ID, video_file, VK_ALBUM_ID, name, description, vk_privacy_view)
            except Exception as e:
                logging.error(f"Ошибка VK: {e}")
                vk_success[0] = False

        def odysee_upload():
            try:
                claim_id = upload_to_odysee(video_file, claim_name, "@unuasha", thumbnail_url, name, description, tags, odysee_visibility, debug)
                odysee_success[0] = claim_id is not None
            except Exception as e:
                logging.error(f"Ошибка Odysee: {e}")
                odysee_success[0] = False

        threads = []
        if do_vk_upload:
            vk_thread = threading.Thread(target=vk_upload)
            threads.append(vk_thread)
            vk_thread.start()
        if do_odysee_upload:
            odysee_thread = threading.Thread(target=odysee_upload)
            threads.append(odysee_thread)
            odysee_thread.start()
        for thread in threads:
            thread.join()

        if (do_vk_upload and vk_success[0]) or (do_odysee_upload and odysee_success[0]):
            if do_odysee_upload:
                stop_lbrynet()
            logging.info(f"Удаляю {video_file}...")
            os.remove(video_file)
            if do_odysee_upload:
                logging.info("Удаляю blobfiles...")
                if os.path.exists(BLOBFILES_PATH):
                    shutil.rmtree(BLOBFILES_PATH, ignore_errors=True)
                start_lbrynet()
        else:
            logging.error(f"Ошибка в строке {index + 1}. Прерываю.")
            if do_odysee_upload:
                stop_lbrynet()
            break

    logging.info("Задача успешно выполнена! Все файлы загружены.")
    if do_odysee_upload:
        stop_lbrynet()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Скрипт для загрузки видео")
    parser.add_argument("--start", type=int, default=1, help="Начальная строка (с 1)")
    parser.add_argument("--end", type=int, help="Конечная строка")
    parser.add_argument("--vk", action="store_true", help="Загружать на VK")
    parser.add_argument("--odysee", action="store_true", help="Загружать на Odysee")
    parser.add_argument("--debug", action="store_true", help="Включить отладочные сообщения")
    args = parser.parse_args()
    do_vk_upload = args.vk or not (args.vk or args.odysee)
    do_odysee_upload = args.odysee or not (args.vk or args.odysee)
    main(args.start, args.end, do_vk_upload, do_odysee_upload, args.debug)
