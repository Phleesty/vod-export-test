#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import json
import math
import time
import shutil
import zipfile
import argparse
import logging
import urllib.request
import subprocess
from datetime import datetime

import pandas as pd
import requests

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow

###############################################################################
# Константы и пути
###############################################################################

CONFIG_FILE = "config.json"
TWITCH_DOWNLOADER_DIR = "./TwitchDownloaderCLI"
TWITCH_DOWNLOADER_PATH = os.path.join(TWITCH_DOWNLOADER_DIR, "TwitchDownloaderCLI")
FFMPEG_PATH = shutil.which("ffmpeg") or "/usr/bin/ffmpeg"
FFPROBE_PATH = shutil.which("ffprobe") or "/usr/bin/ffprobe"
STREAMS_FILE = "streams.xlsx"
CLIENT_SECRETS_FILE = "client_secret.json"
TOKEN_FILE = "token.json"
SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]
MAX_ALLOWED_DURATION = 11 * 3600 + 58 * 60  # 11:58:00

###############################################################################
# Конфиг
###############################################################################

def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except Exception:
                return {}
    return {}

def save_config(config: dict):
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

def setup_vkontakte_config():
    """
    Обеспечивает наличие vk_token, vk_group_id, vk_album_id в config.json (если нужна загрузка в VK).
    """
    config = load_config()
    need = any(not config.get(k) for k in ("vk_token", "vk_group_id", "vk_album_id"))
    if need:
        print("=== Настройка VK ===")
        config["vk_token"] = input("Введите VK access_token: ").strip()
        config["vk_group_id"] = int(input("Введите VK group_id: ").strip())
        config["vk_album_id"] = int(input("Введите VK album_id: ").strip())
        save_config(config)
    return config

def setup_youtube_credentials():
    """
    Сохраняет client_secret.json при необходимости и проводит OAuth если нет token.json.
    """
    if os.path.exists(CLIENT_SECRETS_FILE) and os.path.exists(TOKEN_FILE):
        choice = input("Продолжить с текущими настройками YouTube? (y/n): ").strip().lower()
        if choice == "y":
            return
    print("=== Настройка YouTube ===")
    client_secret = input("Вставьте JSON client_secret (или 'n' для пропуска): ").strip()
    if client_secret.lower() != "n" and "{" in client_secret:
        with open(CLIENT_SECRETS_FILE, "w", encoding="utf-8") as f:
            f.write(client_secret)

###############################################################################
# TwitchDownloader: актуальная версия через GitHub API
###############################################################################

def get_latest_twitch_downloader_url():
    api_url = "https://api.github.com/repos/lay295/TwitchDownloader/releases/latest"
    response = requests.get(api_url, timeout=30)
    response.raise_for_status()
    release_data = response.json()
    for asset in release_data.get("assets", []):
        name = asset.get("name", "")
        if name.endswith("Linux-x64.zip") and name.startswith("TwitchDownloaderCLI-"):
            return asset["browser_download_url"]
    raise RuntimeError("Не найден подходящий TwitchDownloaderCLI для Linux x64")

def ensure_twitch_downloader():
    if not os.path.exists(TWITCH_DOWNLOADER_PATH):
        os.makedirs(TWITCH_DOWNLOADER_DIR, exist_ok=True)
        print("Скачиваю TwitchDownloaderCLI (последняя версия)...")
        url = get_latest_twitch_downloader_url()
        zip_path = os.path.join(TWITCH_DOWNLOADER_DIR, "TwitchDownloaderCLI.zip")
        urllib.request.urlretrieve(url, zip_path)
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(TWITCH_DOWNLOADER_DIR)
        os.remove(zip_path)
        # делаем бинарник исполняемым
        try:
            os.chmod(TWITCH_DOWNLOADER_PATH, 0o755)
        except Exception:
            pass
        print("TwitchDownloaderCLI загружен!")

###############################################################################
# Вспомогательные функции (ffprobe/ffmpeg)
###############################################################################

def get_video_duration(video_file: str) -> float:
    cmd = [
        F FPROBE_PATH, "-v", "error", "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1", video_file
    ]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        return float(result.stdout.strip())
    except Exception:
        return 0.0

def get_chapters(video_file: str):
    cmd = [FFPROBE_PATH, "-v", "quiet", "-print_format", "json", "-show_chapters", video_file]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        data = json.loads(result.stdout)
    except Exception:
        data = {}
    return data.get("chapters", [])

def format_timestamp(seconds: float) -> str:
    seconds = max(0, int(seconds))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02}:{m:02}:{s:02}" if h > 0 else f"{m:02}:{s:02}"

def create_description_from_chapters(chapters) -> str:
    lines = []
    for ch in chapters:
        ts = format_timestamp(float(ch.get("start_time", 0)))
        title = ch.get("tags", {}).get("title", "Untitled")
        lines.append(f"{ts} - {title}")
    return "\n".join(lines)

def create_concat_metadata(video_files):
    cumulative = 0.0
    all_ch = []
    for vf in video_files:
        duration = get_video_duration(vf)
        for ch in get_chapters(vf):
            adjusted = dict(ch)
            adjusted["start_time"] = float(adjusted.get("start_time", 0)) + cumulative
            adjusted["end_time"] = float(adjusted.get("end_time", 0)) + cumulative
            all_ch.append(adjusted)
        cumulative += duration
    content = ";FFMETADATA1\n"
    for ch in all_ch:
        start = int(float(ch["start_time"]) * 1000)
        end = int(float(ch["end_time"]) * 1000)
        title = ch.get("tags", {}).get("title", "Untitled")
        content += f"[CHAPTER]\nTIMEBASE=1/1000\nSTART={start}\nEND={end}\ntitle={title}\n"
    meta_file = "concat_metadata.txt"
    with open(meta_file, "w", encoding="utf-8") as f:
        f.write(content)
    return meta_file

def concatenate_videos(video_files, output_file, metadata_file=None):
    print("Объединяю файлы...")
    with open("concat_list.txt", "w", encoding="utf-8") as f:
        for vf in video_files:
            f.write(f"file '{vf}'\n")
    cmd = [FFMPEG_PATH, "-f", "concat", "-safe", "0", "-i", "concat_list.txt"]
    if metadata_file:
        cmd += ["-i", metadata_file, "-map_metadata", "1"]
    cmd += ["-c", "copy", output_file]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    os.remove("concat_list.txt")
    if metadata_file and os.path.exists(metadata_file):
        os.remove(metadata_file)
    print(f"Видео объединено в {output_file}")

def split_single_video(video_file, max_dur=MAX_ALLOWED_DURATION):
    duration = get_video_duration(video_file)
    if duration <= max_dur:
        return [video_file]
    parts = int(math.ceil(duration / max_dur))
    result = []
    for i in range(parts):
        start = int(i * max_dur)
        part = f"{video_file[:-4]}_part{i+1}.mp4"
        cmd = [FFMPEG_PATH, "-ss", str(start), "-i", video_file, "-t", str(max_dur), "-c", "copy", part]
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        result.append(part)
    return result

###############################################################################
# Twitch API (для -last)
###############################################################################

def _extract_date_from_title(title: str):
    """
    Возвращает ISO8601Z если в названии встречается дата формата DD.MM.YYYY или YYYY-MM-DD.
    """
    if not title:
        return None
    m = re.search(r"(\d{2}\.\d{2}\.\d{4})|(\d{4}-\d{2}-\d{2})", title)
    if not m:
        return None
    date_str = m.group(0)
    try:
        if "." in date_str:
            return datetime.strptime(date_str, "%d.%m.%Y").isoformat() + "Z"
        return datetime.strptime(date_str, "%Y-%m-%d").isoformat() + "Z"
    except ValueError:
        return None

def _get_twitch_credentials():
    """
    Достаёт client_id/secret из env или config.json; при отсутствии — спрашивает и сохраняет.
    """
    cfg = load_config()
    client_id = os.getenv("TWITCH_CLIENT_ID") or cfg.get("twitch_client_id")
    client_secret = os.getenv("TWITCH_CLIENT_SECRET") or cfg.get("twitch_client_secret")
    if not client_id:
        client_id = input("Введите Twitch Client ID: ").strip()
        cfg["twitch_client_id"] = client_id
        save_config(cfg)
    if not client_secret:
        client_secret = input("Введите Twitch Client Secret: ").strip()
        cfg = load_config()
        cfg["twitch_client_secret"] = client_secret
        save_config(cfg)
    return client_id, client_secret

def _get_twitch_token(client_id, client_secret):
    r = requests.post(
        "https://id.twitch.tv/oauth2/token",
        data={"client_id": client_id, "client_secret": client_secret, "grant_type": "client_credentials"},
        timeout=30
    )
    r.raise_for_status()
    return r.json()["access_token"]

def _get_user_id(username, client_id, token):
    r = requests.get(
        f"https://api.twitch.tv/helix/users?login={username}",
        headers={"Client-ID": client_id, "Authorization": f"Bearer {token}"},
        timeout=30
    )
    r.raise_for_status()
    data = r.json().get("data", [])
    if not data:
        raise RuntimeError(f"Пользователь '{username}' не найден в Twitch.")
    return data[0]["id"]

def _fetch_archives(user_id, count, client_id, token):
    """
    Возвращает до `count` архивов (type=archive), отсортированных от старого к новому.
    """
    items = []
    cursor = None
    remaining = count
    while remaining > 0:
        page_size = min(100, remaining)
        url = f"https://api.twitch.tv/helix/videos?user_id={user_id}&first={page_size}&type=archive"
        if cursor:
            url += f"&after={cursor}"
        r = requests.get(url, headers={"Client-ID": client_id, "Authorization": f"Bearer {token}"}, timeout=30)
        r.raise_for_status()
        payload = r.json()
        data = payload.get("data", [])
        items.extend(data)
        remaining -= len(data)
        cursor = payload.get("pagination", {}).get("cursor")
        if not cursor or not data:
            break
        # лёгкий троттлинг на всякий случай
        time.sleep(0.2)
    # сортируем стабильно от старого к новому
    return sorted(items[:count], key=lambda x: x.get("created_at", ""))

def generate_streams_xlsx(username, count, output_file=STREAMS_FILE):
    """
    Формирует streams.xlsx с колонками:
    B — URL, C — Title + (DD.MM.YYYY), D — Description (пусто), E — Tags (пусто),
    F — dd-mm-YYYY, I — chat_filename.json
    """
    client_id, client_secret = _get_twitch_credentials()
    token = _get_twitch_token(client_id, client_secret)
    user_id = _get_user_id(username, client_id, token)
    videos = _fetch_archives(user_id, int(count), client_id, token)

    rows = []
    for v in videos:
        title = v.get("title", "")
        created_at = v.get("created_at", "")
        original_date = _extract_date_from_title(title) or created_at
        dt = datetime.fromisoformat(original_date.replace("Z", ""))
        formatted_title_date = dt.strftime("(%d.%m.%Y)")  # для колонки C
        excel_date = dt.strftime("%d-%m-%Y")               # для колонки F
        title_with_date = f"{title} {formatted_title_date}".strip()
        url = v.get("url") or f"https://www.twitch.tv/videos/{v.get('id')}"
        chat_json = (url.split("/")[-1] or v.get("id", "unknown")) + ".json"

        rows.append({
            "B": url,
            "C": title_with_date,
            "D": "",                 # description — пусто (добавишь при желании)
            "E": "",                 # tags — пусто (через запятую)
            "F": excel_date,
            "I": chat_json
        })

    # порядок колонок фиксируем
    df = pd.DataFrame(rows, columns=["B", "C", "D", "E", "F", "I"])
    df.to_excel(output_file, index=False, engine="openpyxl")
    print(f"Собрано {len(rows)} видео. Таблица сохранена в {output_file}")

###############################################################################
# Загрузка в VK и YouTube
###############################################################################

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
        "privacy_comment": "all",
    }
    rsp = requests.get("https://api.vk.ru/method/video.save", params=params, timeout=60).json()
    if "error" in rsp:
        raise RuntimeError(f"Ошибка VK API: {rsp['error']['error_msg']}")
    upload_url = rsp["response"]["upload_url"]

    # multipart upload
    with open(video_path, "rb") as f:
        try:
            from requests_toolbelt import MultipartEncoder
        except Exception:
            # fallback to standard multipart
            files = {"video_file": ("video_file", f, "video/mp4")}
            up = requests.post(upload_url, files=files, timeout=None)
            if not up.ok:
                raise RuntimeError(f"Ошибка POST upload VK: {up.text}")
        else:
            enc = MultipartEncoder(fields={"video_file": ("video_file", f, "video/mp4")})
            headers = {"Content-Type": enc.content_type}
            up = requests.post(upload_url, data=enc, headers=headers, timeout=None)
            if not up.ok:
                raise RuntimeError(f"Ошибка POST upload VK: {up.text}")

    logging.info(f"{video_path} успешно загружен в VK.")
    return True

def get_authenticated_youtube_service():
    if os.path.exists(TOKEN_FILE):
        credentials = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    else:
        if not os.path.exists(CLIENT_SECRETS_FILE):
            raise RuntimeError("Отсутствует client_secret.json для YouTube OAuth.")
        flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
        credentials = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w", encoding="utf-8") as token:
            token.write(credentials.to_json())
    return build("youtube", "v3", credentials=credentials)

def upload_to_youtube(video_file, title, description, tags):
    print(f"Загружаю {video_file} на YouTube...")
    logging.info(f"Загрузка {video_file} на YouTube")
    start = datetime.now()
    youtube = get_authenticated_youtube_service()
    body = {
        "snippet": {
            "title": title or os.path.basename(video_file),
            "description": description or "",
            "tags": [t.strip() for t in str(tags or "").split(",") if t.strip()],
            "categoryId": "22",
        },
        "status": {"privacyStatus": "private"},
    }
    media = MediaFileUpload(video_file, chunksize=-1, resumable=True)
    request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
    _ = request.execute()
    elapsed = (datetime.now() - start).total_seconds()
    size_mb = os.path.getsize(video_file) / (1024 * 1024)
    print(f"  {video_file} ({size_mb:.2f} MB) загружено на YouTube за {int(elapsed//60)} мин {int(elapsed%60)} сек.")

def add_part_to_title(title, part_number):
    title = title or ""
    last_open = title.rfind("(")
    if last_open == -1:
        return f"{title}. Часть {part_number}".strip()
    date_part = title[last_open:]
    main_title = title[:last_open].strip()
    sep = "" if (main_title and main_title[-1] in ".!?") else "."
    return f"{main_title}{sep} Часть {part_number} {date_part}".strip()

###############################################################################
# Скачивание Twitch-видео (TwitchDownloaderCLI)
###############################################################################

def download_twitch_video(video_url, output_file):
    video_id = video_url.split("/")[-1]
    print(f"Скачиваю из Twitch: {video_url} → {output_file}")
    logging.info(f"Загрузка видео Twitch: {video_url}")
    cmd = [
        TWITCH_DOWNLOADER_PATH, "videodownload",
        "--id", video_id,
        "-o", output_file,
        "--threads", "20",
        "--temp-path", "temp"
    ]
    os.makedirs("temp", exist_ok=True)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    patt = re.compile(r"Downloading\s+(\d+)%")
    while True:
        line = proc.stdout.readline()
        if not line and proc.poll() is not None:
            break
        if line:
            m = patt.search(line)
            if m:
                pct = int(m.group(1))
                print(f"  [{output_file}] {pct:3d}%", end="\r")
    proc.wait()
    print(f"  [{output_file}] 100%                     ")
    logging.info(f"Файл {output_file} скачан.")

###############################################################################
# Основной процесс
###############################################################################

def _pick_first_nonempty(row, indices):
    """
    Возвращает первую непустую строку из указанных позиций iloc.
    """
    for idx in indices:
        if idx < len(row):
            val = row.iloc[idx]
            if pd.notna(val) and str(val).strip():
                return str(val).strip()
    return ""

def _get_link_from_row(row):
    """
    Поддерживает две раскладки:
    - Новая: ссылка в колонке B => iloc[0]
    - Старая: ссылка во 2-й колонке => iloc[1]
    """
    cands = []
    for idx in (0, 1, 2):
        if idx < len(row):
            v = str(row.iloc[idx]) if pd.notna(row.iloc[idx]) else ""
            if "twitch.tv" in v:
                cands.append(v.strip())
    return cands[0] if cands else ""

def main(start_row=1, end_row=None, do_vk=True, do_youtube=True, max_uploads=99, debug=False):
    ensure_twitch_downloader()

    config = load_config()
    vk_cfg = None
    if do_vk:
        vk_cfg = setup_vkontakte_config()
    if do_youtube:
        setup_youtube_credentials()

    logging.basicConfig(
        filename="upload_combined.log",
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    if not os.path.exists(STREAMS_FILE):
        print(f"Не найден {STREAMS_FILE}. Используйте флаг -last <username> <count> для автогенерации.")
        return

    print("Очистка временных .mp4 файлов перед запуском...")
    for f in os.listdir():
        if f.endswith(".mp4"):
            try:
                os.remove(f)
            except Exception:
                pass

    df = pd.read_excel(STREAMS_FILE)
    start_index = max(0, start_row - 1)
    end_index = end_row if end_row is not None else len(df)
    uploaded_count = 0

    for index in range(start_index, end_index):
        row = df.iloc[index]

        # 1) ссылка(и)
        link_cell = _get_link_from_row(row)
        if not link_cell:
            print(f"Строка {index+1}: нет Twitch-ссылки, пропускаю.")
            continue
        video_urls = str(link_cell).split()

        # 2) заголовок (пытаемся взять из C => iloc[1], иначе iloc[2], иначе пусто)
        name = _pick_first_nonempty(row, [1, 2])

        # 3) описание (D => iloc[2] при “новой” разметке)
        description = _pick_first_nonempty(row, [2, 3])

        # 4) теги (E => iloc[3] при “новой” разметке)
        tags = _pick_first_nonempty(row, [3, 4])

        print(f"\n[{index+1}] Обрабатываю…")
        video_files = []
        for url in video_urls:
            video_id = url.split("/")[-1] if "twitch.tv" in url else url
            out_file = f"{video_id}.mp4"
            print(f"-> Скачивание Twitch ID: {video_id}    ({url})")
            download_twitch_video(url, out_file)
            video_files.append(out_file)

        # если несколько — конкат
        if len(video_files) > 1:
            meta = create_concat_metadata(video_files)
            final_file = f"concatenated_{index+1}.mp4"
            concatenate_videos(video_files, final_file, meta)
            for f in video_files:
                try:
                    os.remove(f)
                except Exception:
                    pass
            video_file = final_file
        else:
            video_file = video_files[0]

        # Опционально строим описание из глав
        chapters = get_chapters(video_file)
        description_final = create_description_from_chapters(chapters) if chapters else (description or "")

        # 1. VK
        vk_ok = True
        if do_vk and vk_cfg:
            try:
                print(f"-> Загрузка в VK: {video_file}")
                privacy = "all"  # при желании можно маппить из столбца
                upload_video_to_vk(
                    vk_cfg["vk_token"], vk_cfg["vk_group_id"], video_file,
                    vk_cfg["vk_album_id"], name, description_final, privacy_view=privacy
                )
                print(f"-> VK: файл {video_file} успешно загружен.")
                logging.info(f"VK upload ok for {video_file}")
            except Exception as e:
                print(f"--!! Ошибка загрузки в VK: {e}")
                logging.error(f"Ошибка VK для {video_file}: {e}")
                vk_ok = False

        # 2. YouTube
        if do_youtube and vk_ok:
            to_upload = []
            duration = get_video_duration(video_file)
            if duration > MAX_ALLOWED_DURATION:
                to_upload = split_single_video(video_file)
            else:
                to_upload = [video_file]

            for i, up_file in enumerate(to_upload):
                if uploaded_count >= max_uploads:
                    print("Достигнут лимит YouTube загрузок (max-uploads).")
                    break
                y_chapters = get_chapters(up_file)
                y_desc = create_description_from_chapters(y_chapters) if y_chapters else description_final
                yt_title = add_part_to_title(name, i + 1) if len(to_upload) > 1 else (name or os.path.basename(up_file))
                try:
                    upload_to_youtube(up_file, yt_title, y_desc, tags)
                    print(f"-> YouTube: {up_file} успешно загружен.")
                    logging.info(f"YouTube upload ok for {up_file}")
                    uploaded_count += 1
                except Exception as e:
                    print(f"--!! Ошибка загрузки на YouTube: {e}")
                    logging.error(f"Ошибка YouTube для {up_file}: {e}")

        # 3. Очистка временных файлов
        try:
            files_for_cleanup = set(video_files + ([video_file] if video_file not in video_files else []))
            for f in os.listdir():
                if (f.startswith(video_file[:-4]) and f.endswith(".mp4")) or f in files_for_cleanup:
                    try:
                        os.remove(f)
                    except Exception:
                        pass
            print(f"Удалены все временные файлы для строки {index+1}.")
        except Exception as e:
            print(f"Ошибка при удалении файлов: {e}")

    print("\nВыполнено!\n")

###############################################################################
# CLI
###############################################################################

def parse_args():
    parser = argparse.ArgumentParser(description="Скрипт для групповой загрузки в VK и YouTube")
    parser.add_argument("--start", type=int, default=1, help="Начальная строка (с 1)")
    parser.add_argument("--end", type=int, help="Конечная строка (включительно)")
    parser.add_argument("--vk", action="store_true", help="Загружать только VK")
    parser.add_argument("--youtube", action="store_true", help="Загружать только YouTube")
    parser.add_argument("--max-uploads", type=int, default=99, help="Максимум файлов для YouTube за запуск")
    parser.add_argument("--debug", action="store_true", help="Подробный лог")
    parser.add_argument("-last", "--last", nargs=2, metavar=("USERNAME", "COUNT"),
                        help="Скачать последние COUNT архивов у Twitch-пользователя USERNAME и сформировать streams.xlsx")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()

    # Если вызван режим -last/--last: сначала формируем streams.xlsx
    if args.last:
        username, count = args.last
        generate_streams_xlsx(username=username, count=int(count), output_file=STREAMS_FILE)

    # Логика выбора платформ:
    # - если не выставлено ни одного флага, то загружаем в обе
    do_vk = args.vk or (not args.vk and not args.youtube)
    do_youtube = args.youtube or (not args.vk and not args.youtube)

    main(args.start, args.end, do_vk, do_youtube, args.max_uploads, args.debug)
