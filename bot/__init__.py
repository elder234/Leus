#!/usr/bin/env python3
import asyncio
import os
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from hydrogram import Client as tgClient, enums, filters
from pymongo import MongoClient
from dotenv import load_dotenv, dotenv_values
from logging import getLogger, Formatter, FileHandler, StreamHandler, INFO, error as log_error, info as log_info, warning as log_warning

# Additional imports you may need
from os import path as ospath
from aria2p import API as ariaAPI, Client as ariaClient
from qbittorrentapi import Client as qbClient
from faulthandler import enable as faulthandler_enable
from socket import setdefaulttimeout
from uvloop import install

faulthandler_enable()
install()
setdefaulttimeout(600)

# Basic logging configuration
basicConfig(format="[%(asctime)s] [%(levelname)s] - %(message)s",
            datefmt="%d-%b-%y %I:%M:%S %p",
            handlers=[FileHandler('log.txt'), StreamHandler()],
            level=INFO)

LOGGER = getLogger(__name__)

load_dotenv('config.env', override=True)

# Initialize variables and locks
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

# Locks
download_dict_lock = asyncio.Lock()
status_reply_dict_lock = asyncio.Lock()
queue_dict_lock = asyncio.Lock()
qb_listener_lock = asyncio.Lock()
status_reply_dict = {}
download_dict = {}
rss_dict = {}

# Fetching environment variables
BOT_TOKEN = os.environ.get('BOT_TOKEN', '')
if len(BOT_TOKEN) == 0:
    log_error("BOT_TOKEN variable is missing! Exiting now")
    exit(1)

bot_id = BOT_TOKEN.split(':', 1)[0]

DATABASE_URL = os.environ.get('DATABASE_URL', '')
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
                os.environ[key] = str(value)
        if pf_dict := db.settings.files.find_one({'_id': bot_id}):
            del pf_dict['_id']
            for key, value in pf_dict.items():
                if value:
                    file_ = key.replace('__', '.')
                    with open(file_, 'wb+') as f:
                        f.write(value)
        if a2c_options := db.settings.aria2c.find_one({'_id': bot_id}):
            del a2c_options['_id']
            aria2_options.update(a2c_options)  # Update instead of reassignment
        if qbit_opt := db.settings.qbittorrent.find_one({'_id': bot_id}):
            del qbit_opt['_id']
            qbit_options.update(qbit_opt)  # Update instead of reassignment
        conn.close()

async def main():
    scheduler = AsyncIOScheduler()

    # Initialize database asynchronously
    await initialize_db()

    # Scheduled jobs
    scheduler.add_job(check_download_status, 'interval', seconds=5)
    scheduler.add_job(refresh_torrent_info, 'interval', minutes=15)
    scheduler.add_job(cleanup_old_tasks, 'interval', hours=1)

    scheduler.start()
    log_info("Bot Started!")

    try:
        await bot.start()
        await bot.idle()
    except KeyboardInterrupt:
        pass
    finally:
        await bot.stop()
        log_info("Bot stopped!")

async def check_download_status():
    # Placeholder for download status checking logic
    pass

async def refresh_torrent_info():
    # Placeholder for torrent information refreshing logic
    pass

async def cleanup_old_tasks():
    # Placeholder for old task cleanup logic
    pass

if __name__ == "__main__":
    asyncio.run(main())
