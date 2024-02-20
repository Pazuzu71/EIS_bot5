import asyncio
import logging
import re
import os
import zipfile
import time
from datetime import datetime


import asyncpg
from asyncpg.pool import Pool
import aioftp
import pytz
from aiogram import Bot, Dispatcher
from aiogram import F
from aiogram.filters import CommandStart
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder
from apscheduler.schedulers.asyncio import AsyncIOScheduler


from config import TOKEN, credentials, host, port, login, password
from app import main


# Настраиваем базовую конфигурацию логирования
logging.basicConfig(
    format='[%(asctime)s] #%(levelname)-8s %(filename)s: %(lineno)d - %(name)s:%(funcName)s - %(message)s',
    level=logging.INFO,
)
# Инициализируем логгер модуля
logger = logging.getLogger(__name__)
# Устанавливаем логгеру уровень `DEBUG`
logger.setLevel(logging.DEBUG)
# Инициализируем хэндлер, который будет писать логи в файл `error.log`
error_file = logging.FileHandler('error.log', 'a', encoding='utf-8')
# Устанавливаем хэндлеру уровень `DEBUG`
error_file.setLevel(logging.DEBUG)
# Инициализируем форматтер
formatter_1 = logging.Formatter(
    fmt='[%(asctime)s] #%(levelname)-8s %(filename)s:'
        '%(lineno)d - %(name)s:%(funcName)s - %(message)s'
)
# Определяем форматирование логов в хэндлере
error_file.setFormatter(formatter_1)
# Добавляем хэндлер в логгер
logger.addHandler(error_file)


def kb_creator(documents):
    documents = sorted([(dt.astimezone(tz=pytz.timezone('Europe/Moscow')), id_) for dt, id_ in documents], reverse=True)
    print(documents)
    buttons = [
        InlineKeyboardButton(text=document[0].strftime('%Y-%m-%d %H:%M'),
                             callback_data=f'document_{document[1]}') for document in documents
    ]
    kb_builder = InlineKeyboardBuilder()
    kb_builder.row(width=3, *buttons)
    return kb_builder.as_markup()


async def get_psql_data(pool: Pool, id_: int):
    conn = None
    try:
        async with pool.acquire() as conn:
            result = await conn.fetchrow(
                """
                SELECT zip.ftp_path, xml.xmlname
                FROM zip 
                INNER JOIN xml on zip.zip_id = xml.zip_id 
                WHERE xml.xml_id = $1;
                """,
                id_
            )
            # print('result', result, id_)
            return result

    except Exception as e:
        print(f"An error occurred: {e}", Exception)
    finally:
        if conn:
            await pool.release(conn)


async def find_psql_document_id(pool: Pool, eisdocno: str):
    conn = None
    # while True:
    try:
        async with pool.acquire() as conn:
            result = await conn.fetch(
                """
                SELECT eispublicationdate, xml_id
                FROM zip 
                INNER JOIN xml on zip.zip_id = xml.zip_id 
                WHERE zip.enddate IS NULL AND xml.eisdocno = $1;
                """,
                eisdocno
            )
            return result
    except Exception as e:
        print(f"An error occurred: {e}", Exception)
    finally:
        if conn:
            await pool.release(conn)


async def worker(queue: asyncio.Queue, bot: Bot):
    while True:
        user_id, message_id, ftp_path, xmlname = await queue.get()
        await bot.send_message(chat_id=user_id, text=f'{user_id}, {message_id}, {ftp_path}, {xmlname}')
        file = ftp_path.split('/')[-1]
        if all([user_id, message_id, ftp_path, xmlname]):
            while True:
                client = None
                try:
                    async with aioftp.Client().context(host, port, login, password) as client:
                        await client.download(ftp_path, f"Temp/{user_id}/{file}", write_into=True)
                    with zipfile.ZipFile(f"Temp/{user_id}/{file}") as z:
                        z.extract(xmlname, f"Temp/{user_id}/")
                    sending_file = FSInputFile(f"Temp/{user_id}/{xmlname}")
                    await bot.send_document(chat_id=user_id, document=sending_file, reply_to_message_id=message_id)
                    time.sleep(1)
                    os.unlink(f"Temp/{user_id}/{file}")
                    os.unlink(f"Temp/{user_id}/{xmlname}")
                    break
                except ConnectionResetError:
                    if os.path.exists(f"Temp/{user_id}/{file}"):
                        os.unlink(f"Temp/{user_id}/{file}")
                finally:
                    if client:
                        client.close()


async def start_bot():
    logger.info('Запуск бота')
    pool = await asyncpg.create_pool(**credentials)
    queue = asyncio.Queue(maxsize=100)
    bot = Bot(token=TOKEN)
    dp = Dispatcher()

    @dp.message(CommandStart())
    async def start(msg: Message):
        logger.info('это старт хэндлер')
        await msg.answer('это эхо')

    @dp.message(lambda msg: re.fullmatch(r'\d{18}', msg.text))
    async def get_over_here(msg: Message):
        logger.info('Перехвачено хэнлером, определяющим номер ЕИС 18 цифр')
        # async with asyncpg.create_pool(**credentials) as pool:
        documents = await find_psql_document_id(pool, msg.text)
        if not documents:
            await msg.reply(f'В базе нет информации по плану-графику с реестровым номером {msg.text}')
        else:
            kb = kb_creator(documents)
            await msg.reply(text=msg.text, reply_markup=kb)

    @dp.message(lambda msg: re.fullmatch(r'\d{23}', msg.text))
    async def get_over_here(msg: Message):
        logger.info('Перехвачено хэнлером, определяющим номер ЕИС 19 цифр')
        documents = await find_psql_document_id(pool, msg.text)
        if not documents:
            await msg.reply(f'В базе нет информации по проекту контракта с реестровым номером {msg.text}')
        if len(documents) == 1:
            id_ = documents[0][-1]
            ftp_path, xmlname = await get_psql_data(pool, int(id_))
            await queue.put((msg.from_user.id, msg.message_id, ftp_path, xmlname))
        else:
            kb = kb_creator(documents)
            await msg.reply(text=msg.text, reply_markup=kb)

    @dp.callback_query(F.data.startswith('document_'))
    async def get_document(callback: CallbackQuery):
        id_ = callback.data.split('_')[-1]
        await callback.answer(text=f'id файла в базе {id_}')
        # async with asyncpg.create_pool(**credentials) as pool:
        ftp_path, xmlname = await get_psql_data(pool, int(id_))
        await queue.put((callback.from_user.id, callback.message.message_id, ftp_path, xmlname))

    @dp.message()
    async def echo(msg: Message):
        logger.info('это эхо хэндлер')
        await msg.reply(msg.text)

    scheduler = AsyncIOScheduler(timezone='Europe/Moscow')
    scheduler.add_job(main, args=[pool], trigger='interval', minutes=60, next_run_time=datetime.now())
    scheduler.start()

    # await worker(queue, bot)

    # очищаем очередь апдейтов, запускаем поулинг
    await bot.delete_webhook(drop_pending_updates=True)
    # await dp.start_polling(bot)
    async with asyncio.TaskGroup() as tg:
        tg.create_task(dp.start_polling(bot))
        tg.create_task(worker(queue, bot), name='worker-0')
        tg.create_task(worker(queue, bot), name='worker-1')


if __name__ == '__main__':
    try:
        logger.info('запуск мэйна 3 2 1..')
        asyncio.run(start_bot())
    except (KeyboardInterrupt, SystemExit):
        print('Ошибка, остановка бота!')