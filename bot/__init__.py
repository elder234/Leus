#!/usr/bin/env python3
import asyncio
from tzlocal import get_localzone
from pytz import timezone
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from hydrogram import Client as tgClient, enums
from pymongo import MongoClient
from asyncio import Lock
from dotenv import load_dotenv, dotenv_values
from threading import Thread
from time import sleep, time
from subprocess import Popen, run as srun
from os import remove as osremove, path as ospath, environ, getcwd
from aria2p import API as ariaAPI, Client as ariaClient
from qbittorrentapi import Client as qbClient
from faulthandler import enable as faulthandler_enable
from socket import setdefaulttimeout
from logging import getLogger, Formatter, FileHandler, StreamHandler, INFO, basicConfig, error as log_error, info as log_info, warning as log_warning
from uvloop import install

faulthandler_enable()
install()
setdefaulttimeout(600)

botStartTime = time()

basicConfig(format="[%(asctime)s] [%(levelname)s] - %(message)s", #  [%(filename)s:%(lineno)d]
            datefmt="%d-%b-%y %I:%M:%S %p",
            handlers=[FileHandler('log.txt'), StreamHandler()],
            level=INFO)

LOGGER = getLogger(__name__)

load_dotenv('config.env', override=True)

Interval = []
QbInterval = []
QbTorrents = {}
GLOBAL_EXTENSION_FILTER = ['aria2', '!qB']
user_data = {}
extra_buttons = {}
list_drives_dict = {}
shorteners_list = []
categories_dict = {}
aria2_options = {}
qbit_options = {}
queued_dl = {}
queued_up = {}
bot_cache = {}
non_queued_dl = set()
non_queued_up = set()

try:
    if bool(environ.get('_____REMOVE_THIS_LINE_____')):
        log_error('The README.md file there to be read! Exiting now!')
        exit()
except:
    pass

download_dict_lock = Lock()
status_reply_dict_lock = Lock()
queue_dict_lock = Lock()
qb_listener_lock = Lock()
status_reply_dict = {}
download_dict = {}
rss_dict = {}

BOT_TOKEN = environ.get('BOT_TOKEN', '')
if len(BOT_TOKEN) == 0:
    log_error("BOT_TOKEN variable is missing! Exiting now")
    exit(1)

bot_id = BOT_TOKEN.split(':', 1)[0]

DATABASE_URL = environ.get('DATABASE_URL', '')
if len(DATABASE_URL) == 0:
    DATABASE_URL = ''

async def initialize_db():
    if DATABASE_URL:
        conn = MongoClient(DATABASE_URL)
        db = conn.wzmlx
        current_config = dict(dotenv_values('config.env'))
        old_config = db.settings.deployConfig.find_one({'_id': bot_id})
        if old_config is None:
            db.settings.deployConfig.replace_one(
                {'_id': bot_id}, current_config, upsert=True)
        else:
            del old_config['_id']
        if old_config and old_config != current_config:
            db.settings.deployConfig.replace_one(
                {'_id': bot_id}, current_config, upsert=True)
        elif config_dict := db.settings.config.find_one({'_id': bot_id}):
            del config_dict['_id']
            for key, value in config_dict.items():
                environ[key] = str(value)
        if pf_dict := db.settings.files.find_one({'_id': bot_id}):
            del pf_dict['_id']
            for key, value in pf_dict.items():
                if value:
                    file_ = key.replace('__', '.')
                    with open(file_, 'wb+') as f:
                        f.write(value)
        if a2c_options := db.settings.aria2c.find_one({'_id': bot_id}):
            del a2c_options['_id']
            aria2_options = a2c_options
        if qbit_opt := db.settings.qbittorrent.find_one({'_id': bot_id}):
            del qbit_opt['_id']
            qbit_options = qbit_opt
        conn.close()
        BOT_TOKEN = environ.get('BOT_TOKEN', '')
        bot_id = BOT_TOKEN.split(':', 1)[0]
        DATABASE_URL = environ.get('DATABASE_URL', '')

await initialize_db()

OWNER_ID = environ.get('OWNER_ID', '')
if len(OWNER_ID) == 0:
    log_error("OWNER_ID variable is missing! Exiting now")
    exit(1)
else:
    OWNER_ID = int(OWNER_ID)

TELEGRAM_API = environ.get('TELEGRAM_API', '')
if len(TELEGRAM_API) == 0:
    log_error("TELEGRAM_API variable is missing! Exiting now")
    exit(1)
else:
    TELEGRAM_API = int(TELEGRAM_API)

TELEGRAM_HASH = environ.get('TELEGRAM_HASH', '')
if len(TELEGRAM_HASH) == 0:
    log_error("TELEGRAM_HASH variable is missing! Exiting now")
    exit(1)

TIMEZONE = environ.get('TIMEZONE', '')
if len(TIMEZONE) == 0:
    TIMEZONE = 'Asia/Kolkata'

def changetz(*args):
    return datetime.now(timezone(TIMEZONE)).timetuple()
Formatter.converter = changetz
log_info("TIMEZONE synced with logging status")

GDRIVE_ID = environ.get('GDRIVE_ID', '')
if len(GDRIVE_ID) == 0:
    GDRIVE_ID = ''

RCLONE_PATH = environ.get('RCLONE_PATH', '')
if len(RCLONE_PATH) == 0:
    RCLONE_PATH = ''

RCLONE_FLAGS = environ.get('RCLONE_FLAGS', '')
if len(RCLONE_FLAGS) == 0:
    RCLONE_FLAGS = ''

DEFAULT_UPLOAD = environ.get('DEFAULT_UPLOAD', '')
if DEFAULT_UPLOAD != 'rc' and DEFAULT_UPLOAD != 'ddl':
    DEFAULT_UPLOAD = 'gd'

DOWNLOAD_DIR = environ.get('DOWNLOAD_DIR', '')
if len(DOWNLOAD_DIR) == 0:
    DOWNLOAD_DIR = '/usr/src/app/downloads/'
elif not DOWNLOAD_DIR.endswith("/"):
    DOWNLOAD_DIR = f'{DOWNLOAD_DIR}/'

AUTHORIZED_CHATS = environ.get('AUTHORIZED_CHATS', '')
if AUTHORIZED_CHATS:
    aid = AUTHORIZED_CHATS.split()
    for id_ in aid:
        chat_id, *topic_ids = id_.split(':')
        chat_id = int(chat_id)
        user_data.setdefault(chat_id, {'is_auth': True})
        if topic_ids:
            user_data[chat_id].setdefault('topic_ids', []).extend(map(int, topic_ids))

SUDO_USERS = environ.get('SUDO_USERS', '')
if len(SUDO_USERS) != 0:
    aid = SUDO_USERS.split()
    for id_ in aid:
        user_data[int(id_.strip())] = {'is_sudo': True}

BLACKLIST_USERS = environ.get('BLACKLIST_USERS', '')
if len(BLACKLIST_USERS) != 0:
    for id_ in BLACKLIST_USERS.split():
        user_data[int(id_.strip())] = {'is_blacklist': True}

EXTENSION_FILTER = environ.get('EXTENSION_FILTER', '')
if len(EXTENSION_FILTER) > 0:
    fx = EXTENSION_FILTER.split()
    for x in fx:
        x = x.lstrip('.')
        GLOBAL_EXTENSION_FILTER.append(x.strip().lower())

LINKS_LOG_ID = environ.get('LINKS_LOG_ID', '')
LINKS_LOG_ID = '' if len(LINKS_LOG_ID) == 0 else int(LINKS_LOG_ID)

MIRROR_LOG_ID = environ.get('MIRROR_LOG_ID', '')
if len(MIRROR_LOG_ID) == 0:
    MIRROR_LOG_ID = ''

LEECH_LOG_ID = environ.get('LEECH_LOG_ID', '')
if len(LEECH_LOG_ID) == 0:
    LEECH_LOG_ID = ''

EXCEP_CHATS = environ.get('EXCEP_CHATS', '')
if len(EXCEP_CHATS) == 0:
    EXCEP_CHATS = ''

IS_PREMIUM_USER = False
user = ''
USER_SESSION_STRING = environ.get('USER_SESSION_STRING', '')
if len(USER_SESSION_STRING) != 0:
    log_info("Creating client from USER_SESSION_STRING")
    try:
        user = await tgClient('user', TELEGRAM_API, TELEGRAM_HASH, session_string=USER_SESSION_STRING,
                        parse_mode=enums.ParseMode.HTML, no_updates=True, max_concurrent_transmissions=1000).start()
        IS_PREMIUM_USER = user.me.is_premium
    except Exception as e:
        log_error(f"Failed making client from USER_SESSION_STRING : {e}")
        user = ''

MEGA_EMAIL = environ.get('MEGA_EMAIL', '')
MEGA_PASSWORD = environ.get('MEGA_PASSWORD', '')
if len(MEGA_EMAIL) == 0 or len(MEGA_PASSWORD) == 0:
    log_warning('MEGA Credentials not provided!')
    MEGA_EMAIL = ''
    MEGA_PASSWORD = ''

GDTOT_CRYPT = environ.get('GDTOT_CRYPT', '')
if len(GDTOT_CRYPT) == 0:
    GDTOT_CRYPT = ''

JIODRIVE_TOKEN = environ.get('JIODRIVE_TOKEN', '')
if len(JIODRIVE_TOKEN) == 0:
    JIODRIVE_TOKEN = ''

HUBDRIVE_CRYPT = environ.get('HUBDRIVE_CRYPT', '')
if len(HUBDRIVE_CRYPT) == 0:
    HUBDRIVE_CRYPT = ''

KOLOP_CRYPT = environ.get('KOLOP_CRYPT', '')
if len(KOLOP_CRYPT) == 0:
    KOLOP_CRYPT = ''

SHARERPW_CRYPT = environ.get('SHARERPW_CRYPT', '')
if len(SHARERPW_CRYPT) == 0:
    SHARERPW_CRYPT = ''

HUBDRIVE_EMAIL = environ.get('HUBDRIVE_EMAIL', '')
HUBDRIVE_PASS = environ.get('HUBDRIVE_PASS', '')
if len(HUBDRIVE_EMAIL) == 0 or len(HUBDRIVE_PASS) == 0:
    HUBDRIVE_EMAIL = ''
    HUBDRIVE_PASS = ''

TORRENT_TIMEOUT = environ.get('TORRENT_TIMEOUT', '')
if len(TORRENT_TIMEOUT) == 0:
    TORRENT_TIMEOUT = 600
else:
    TORRENT_TIMEOUT = int(TORRENT_TIMEOUT)

QUEUE_ALL = environ.get('QUEUE_ALL', '')
if len(QUEUE_ALL) == 0:
    QUEUE_ALL = False
else:
    QUEUE_ALL = True

INCOMPLETE_TASK_NOTIFIER = environ.get('INCOMPLETE_TASK_NOTIFIER', '')
if len(INCOMPLETE_TASK_NOTIFIER) == 0:
    INCOMPLETE_TASK_NOTIFIER = False
else:
    INCOMPLETE_TASK_NOTIFIER = True

BASE_URL = environ.get('BASE_URL', '').rstrip("/")
if len(BASE_URL) == 0:
    BASE_URL = ''

WEB_PINCODE = environ.get('WEB_PINCODE', '')
if len(WEB_PINCODE) == 0:
    WEB_PINCODE = False
else:
    WEB_PINCODE = True

BOT_MAX_TASKS = environ.get('BOT_MAX_TASKS', '')
if len(BOT_MAX_TASKS) == 0:
    BOT_MAX_TASKS = False
else:
    BOT_MAX_TASKS = int(BOT_MAX_TASKS)

SHOW_LIMITS = environ.get('SHOW_LIMITS', '')
if len(SHOW_LIMITS) == 0:
    SHOW_LIMITS = False
else:
    SHOW_LIMITS = True

DAILY_TASK_LIMIT = environ.get('DAILY_TASK_LIMIT', '')
if len(DAILY_TASK_LIMIT) == 0:
    DAILY_TASK_LIMIT = 0
else:
    DAILY_TASK_LIMIT = int(DAILY_TASK_LIMIT)

DAILY_MIRROR_LIMIT = environ.get('DAILY_MIRROR_LIMIT', '')
if len(DAILY_MIRROR_LIMIT) == 0:
    DAILY_MIRROR_LIMIT = 0
else:
    DAILY_MIRROR_LIMIT = int(DAILY_MIRROR_LIMIT)

DAILY_LEECH_LIMIT = environ.get('DAILY_LEECH_LIMIT', '')
if len(DAILY_LEECH_LIMIT) == 0:
    DAILY_LEECH_LIMIT = 0
else:
    DAILY_LEECH_LIMIT = int(DAILY_LEECH_LIMIT)

STORAGE_THRESHOLD = environ.get('STORAGE_THRESHOLD', '')
if len(STORAGE_THRESHOLD) == 0:
    STORAGE_THRESHOLD = 0
else:
    STORAGE_THRESHOLD = int(STORAGE_THRESHOLD)

RCLONE_SERVE_URL = environ.get('RCLONE_SERVE_URL', '')
if len(RCLONE_SERVE_URL) == 0:
    RCLONE_SERVE_URL = ''

LEECH_SPLIT_SIZE = environ.get('LEECH_SPLIT_SIZE', '')
if len(LEECH_SPLIT_SIZE) == 0:
    LEECH_SPLIT_SIZE = 2097152000
else:
    LEECH_SPLIT_SIZE = int(LEECH_SPLIT_SIZE)

ARIA_PORT = environ.get('ARIA_PORT', '')
if len(ARIA_PORT) == 0:
    ARIA_PORT = 6800
else:
    ARIA_PORT = int(ARIA_PORT)

qb_client = qbClient(host='localhost', port=8090,
                    username=environ.get('QBITTORRENT_USERNAME', ''),
                    password=environ.get('QBITTORRENT_PASSWORD', ''))

aria2 = ariaAPI(ariaClient(
    host='http://localhost', port=ARIA_PORT, secret=''))

DOWNLOAD_STATUS_UPDATE_INTERVAL = 5

if not ospath.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)

async def add_aria2c_torrent(torrent_url, download_path):
    options = aria2_options.copy()
    if download_path:
        options['dir'] = download_path
    torrent_download = await aria2.add_torrent(torrent_url, options=options)
    return torrent_download

async def add_qbit_torrent(torrent_url, download_path):
    options = qbit_options.copy()
    if download_path:
        options['savepath'] = download_path
    torrent_download = await qb_client.torrents_add(torrent_files=torrent_url, save_path=download_path)
    return torrent_download

async def main():
    scheduler = AsyncIOScheduler()
    await initialize_db()

    # Your scheduled jobs and other async tasks go here

    scheduler.start()
    log_info("Bot Started!")
    try:
        await asyncio.Future()
    except KeyboardInterrupt:
        pass
    finally:
        log_info("Bot stopped!")

if __name__ == "__main__":
    asyncio.run(main())
