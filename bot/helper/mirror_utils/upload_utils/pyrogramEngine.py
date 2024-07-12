#!/usr/bin/env python3
from traceback import format_exc
from logging import getLogger, ERROR
from aiofiles.os import remove as aioremove, path as aiopath, rename as aiorename, makedirs, rmdir, mkdir
from os import walk, path as ospath
from time import time
from PIL import Image
from hydrogram.types import InputMediaVideo, InputMediaDocument, InlineKeyboardMarkup
from hydrogram.errors import FloodWait, RPCError, PeerIdInvalid, ChannelInvalid
from asyncio import sleep
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type, RetryError
from re import match as re_match, sub as re_sub
from natsort import natsorted
from aioshutil import copy

from bot import config_dict, user_data, GLOBAL_EXTENSION_FILTER, bot, user, IS_PREMIUM_USER
from bot.helper.themes import BotTheme
from bot.helper.telegram_helper.button_build import ButtonMaker
from bot.helper.telegram_helper.message_utils import sendCustomMsg, editReplyMarkup, sendMultiMessage, chat_info, deleteMessage, get_tg_link_content
from bot.helper.ext_utils.fs_utils import clean_unwanted, is_archive, get_base_name
from bot.helper.ext_utils.bot_utils import get_readable_file_size, is_telegram_link, is_url, sync_to_async, download_image_url
from bot.helper.ext_utils.leech_utils import get_audio_thumb, get_media_info, get_document_type, take_ss, get_ss, get_mediainfo_link, format_filename

LOGGER = getLogger(__name__)
getLogger("hydrogram").setLevel(ERROR)


class TgUploader:

    def __init__(self, name=None, path=None, listener=None):
        self.name = name
        self.__last_uploaded = 0
        self.__processed_bytes = 0
        self.__listener = listener
        self.__path = path
        self.__start_time = time()
        self.__total_files = 0
        self.__is_cancelled = False
        self.__retry_error = False
        self.__thumb = f"Thumbnails/{listener.message.from_user.id}.jpg"
        self.__sent_msg = None
        self.__has_buttons = False
        self.__msgs_dict = {}
        self.__corrupted = 0
        self.__is_corrupted = False
        self.__media_dict = {'videos': {}, 'documents': {}}
        self.__last_msg_in_group = False
        self.__prm_media = False
        self.__client = bot
        self.__up_path = ''
        self.__mediainfo = False
        self.__as_doc = False
        self.__media_group = False
        self.__upload_dest = ''
        self.__bot_pm = False
        self.__user_id = listener.message.from_user.id
        self.__leechmsg = {}
        self.__leech_utils = self.__listener.leech_utils
        
    async def get_custom_thumb(self, thumb):
        if is_telegram_link(thumb):
            try:
                msg, client = await get_tg_link_content(thumb, self.__user_id )
            except Exception as e:
                LOGGER.error(f"Thumb Access Error: {e}")
                return None
            if msg and not msg.photo:
                LOGGER.error("Thumb TgLink Invalid: Provide Link to Photo Only !")
                return None
            _client = bot if client == 'bot' else user
            photo_dir = await _client.download_media(msg)
        elif is_url(thumb):
            photo_dir = await download_image_url(thumb)
        else:
            LOGGER.error("Custom Thumb Invalid")
            return None
        if await aiopath.exists(photo_dir):
            path = "Thumbnails"
            if not await aiopath.isdir(path):
                await mkdir(path)
            des_dir = ospath.join(path, f'{time()}.jpg')
            await sync_to_async(Image.open(photo_dir).convert("RGB").save, des_dir, "JPEG")
            await aioremove(photo_dir)
            return des_dir
        return None

    async def __buttons(self, up_path, is_video=False):
        buttons = ButtonMaker()
        try:
            if config_dict['SCREENSHOTS_MODE'] and is_video and bool(self.__leech_utils['screenshots']):
                buttons.ubutton(BotTheme('SCREENSHOTS'), await get_ss(up_path, self.__leech_utils['screenshots']))
        except Exception as e:
            LOGGER.error(f"ScreenShots Error: {e}")
        try:
            if self.__mediainfo:
                buttons.ubutton(BotTheme('MEDIAINFO_LINK'), await get_mediainfo_link(up_path))
        except Exception as e:
            LOGGER.error(f"MediaInfo Error: {e}")
        if config_dict['SAVE_MSG'] and (config_dict['LEECH_LOG_ID'] or not self.__listener.isPrivate):
            buttons.ibutton(BotTheme('SAVE_MSG'), 'save', 'footer')
        if self.__has_buttons:
            return buttons.build_menu(1)
        return None

    async def __copy_file(self):
        try:
            if self.__bot_pm and (self.__leechmsg and not self.__listener.excep_chat or self.__listener.isSuperGroup):
                copied = await bot.copy_message(
                    chat_id=self.__user_id,
                    from_chat_id=self.__sent_msg.chat.id,
                    message_id=self.__sent_msg.id,
                    reply_to_message_id=self.__listener.botpmmsg.id if self.__listener.botpmmsg else None
                )
                if copied and self.__has_buttons:
                    btn_markup = InlineKeyboardMarkup(BTN) if (BTN := self.__sent_msg.reply_markup.inline_keyboard[:-1]) else None
                    await editReplyMarkup(copied, btn_markup if config_dict['SAVE_MSG'] else self.__sent_msg.reply_markup)
        except Exception as err:
            if not self.__is_cancelled:
                LOGGER.error(f"Failed To Send in BotPM:\n{str(err)}")
        
        try:
            if len(self.__leechmsg) > 1 and not self.__listener.excep_chat:
                for chat_id, msg in list(self.__leechmsg.items())[1:]:
                    chat_id, *topics = chat_id.split(':')
                    leech_copy = await bot.copy_message(
                        chat_id=int(chat_id),
                        from_chat_id=self.__sent_msg.chat.id,
                        message_id=self.__sent_msg.id,
                        reply_to_message_id=msg.id
                    )
                    # Layer 161 Needed for Topics !
                    if config_dict['CLEAN_LOG_MSG'] and msg.text:
                        await deleteMessage(msg)
                    if leech_copy and self.__has_buttons:
                        await editReplyMarkup(leech_copy, self.__sent_msg.reply_markup)
        except Exception as err:
            if not self.__is_cancelled:
                LOGGER.error(f"Failed To Send in Leech Log [ {chat_id} ]:\n{str(err)}")
        
        try:
            if self.__upload_dest:
                for channel_id in self.__upload_dest:
                    if chat := (await chat_info(channel_id)):
                        try:
                            dump_copy = await bot.copy_message(
                                chat_id=chat.id,
                                from_chat_id=self.__sent_msg.chat.id,
                                message_id=self.__sent_msg.id
                            )
                            if dump_copy and self.__has_buttons:
                                btn_markup = InlineKeyboardMarkup(BTN) if (BTN := self.__sent_msg.reply_markup.inline_keyboard[:-1]) else None
                                await editReplyMarkup(dump_copy, btn_markup if config_dict['SAVE_MSG'] else self.__sent_msg.reply_markup)
                        except (ChannelInvalid, PeerIdInvalid) as e:
                            LOGGER.error(f"{e.NAME}: {e.MESSAGE} for {channel_id}")
                            continue
        except Exception as err:
            if not self.__is_cancelled:
                LOGGER.error(f"Failed To Send in User Dump:\n{str(err)}")


    async def __upload_progress(self, current, total):
        if self.__is_cancelled:
            if IS_PREMIUM_USER:
                user.stop_transmission()
            bot.stop_transmission()
        chunk_size = current - self.__last_uploaded
        self.__last_uploaded = current
        self.__processed_bytes += chunk_size

    async def __user_settings(self):
        user_dict = user_data.get(self.__user_id, {})
        self.__as_doc = user_dict.get('as_doc', False) or (config_dict['AS_DOCUMENT'] if 'as_doc' not in user_dict else False)
        self.__media_group = user_dict.get('media_group') or (config_dict['MEDIA_GROUP'] if 'media_group' not in user_dict else False)
        self.__bot_pm = user_dict.get('bot_pm') or (config_dict['BOT_PM'] if 'bot_pm' not in user_dict else False)
        self.__mediainfo = user_dict.get('mediainfo') or (config_dict['SHOW_MEDIAINFO'] if 'mediainfo' not in user_dict else False)
        self.__upload_dest = ud if (ud:=self.__listener.upPath) and isinstance(ud, list) else [ud]
        self.__has_buttons = bool(config_dict['SAVE_MSG'] or self.__mediainfo or self.__leech_utils['screenshots'])
        if not await aiopath.exists(self.__thumb):
            self.__thumb = None

    async def __msg_to_reply(self):
        msg_link = self.__listener.message.link if self.__listener.isSuperGroup else ''
        msg_user = self.__listener.message.from_user
        if config_dict['LEECH_LOG_ID'] and not self.__listener.excep_chat:
            try:
                chat_id, *topics = config_dict['LEECH_LOG_ID'].split(':')
                msg = await sendMultiMessage(bot, chat_id, self.__listener.message, msg_link, msg_user, topics=topics)
                self.__leechmsg.update({config_dict['LEECH_LOG_ID']: msg})
            except Exception as e:
                LOGGER.error(f"Leech Log Error: {e}")
        if self.__bot_pm and not self.__listener.isSuperGroup:
            try:
                self.__listener.botpmmsg = await sendCustomMsg(self.__user_id, self.__listener.message)
            except Exception as e:
                LOGGER.error(f"Bot PM Error: {e}")
        if not self.__listener.isPrivate:
            self.__leechmsg.update({str(self.__listener.message.chat.id): self.__listener.message})

    async def __upload(self, o_files, m_size, size):
        await self.__user_settings()
        self.__total_files = len(o_files)
        await self.__msg_to_reply()
        for i in range(len(o_files)):
            if self.__is_cancelled:
                return
            f_name, up_path = o_files[i]
            f_name = format_filename(f_name, self.__listener, self.__total_files, index=i+1)
            up_path = await self.__up_path_func(up_path, f_name, size)
            await self.__send_file(up_path, f_name, o_files[i][1], m_size)

    async def __up_path_func(self, up_path, f_name, size):
        if not self.__leech_utils['up_dir'] and ospath.basename(up_path) != f_name:
            up_path = ospath.join(self.__path, f_name)
            await aiorename(o_files[i][0], up_path)
        if (self.__listener.isSuperGroup or self.__listener.excep_chat) and not self.__leech_utils['up_dir']:
            self.__up_path = f"{self.__listener.message.link}?utm_source=transfer&filename={f_name}&size={size}"
        return up_path

    @retry(wait=wait_exponential(multiplier=1, min=4, max=8), stop=stop_after_attempt(5), retry=retry_if_exception_type(Exception))
    async def __send_file(self, up_path, f_name, o_file, m_size):
        if self.__is_cancelled:
            return
        start_time = time()
        reply = self.__listener.message
        try:
            caption = f'{f_name} ({get_readable_file_size(m_size)})\n\n'
            if reply.from_user.username:
                caption += f'@{reply.from_user.username} '
            caption += f'by @{config_dict["USERBOT_NAME"]}'

            if self.__as_doc:
                input_media = InputMediaDocument(media=up_path, thumb=self.__thumb, caption=caption, parse_mode='html')
            else:
                input_media = InputMediaVideo(media=up_path, thumb=self.__thumb, caption=caption, parse_mode='html')

            media_msg = await user.send_media(self.__client, self.__listener.message.chat.id, input_media, progress=self.__upload_progress)
            
            self.__sent_msg = media_msg

            if not self.__media_group:
                await self.__copy_file()
            
            if not self.__is_cancelled and (elapsed := time() - start_time) < 15:
                await sleep(15 - elapsed)
                
        except FloodWait as f:
            LOGGER.warning(f"FloodWait: Sleeping for {f.x} seconds")
            await sleep(f.x)
            raise
        except RPCError as e:
            LOGGER.error(f"RPCError: {e}")
            raise
        except Exception as e:
            self.__retry_error = True
            LOGGER.error(f"Exception: {e}")
            raise

    async def cancel_upload(self):
        self.__is_cancelled = True
        await self.__listener.onUploadError("Upload cancelled.")

    async def start(self, o_files, m_size, size):
        try:
            await self.__upload(o_files, m_size, size)
        except Exception as e:
            LOGGER.error(f"Start upload error: {e}")
            await self.__listener.onUploadError(f"Error: {e}")
        finally:
            await self.cleanup()

    async def cleanup(self):
        if self.__thumb and await aiopath.exists(self.__thumb):
            await aioremove(self.__thumb)
        if self.__path and await aiopath.exists(self.__path):
            await rmdir(self.__path)
