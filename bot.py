import asyncio
import os
import zipfile
import time
from datetime import datetime


import asyncpg
import aioftp
from aiogram import Bot, Dispatcher
from aiogram.types import FSInputFile
from apscheduler.schedulers.asyncio import AsyncIOScheduler


from log.funcs import create_logger
from config import TOKEN, credentials, host, port, login, password
from app import main
from handlers import users, others


logger = create_logger(__name__)


async def worker(queue: asyncio.Queue, bot: Bot):
    while True:
        user_id, message_id, ftp_path, xmlname = await queue.get()
        # await bot.send_message(chat_id=user_id, text=f'{user_id}, {message_id}, {ftp_path}, {xmlname}')
        file = ftp_path.split('/')[-1]
        if all([user_id, message_id, ftp_path, xmlname]):
            while True:
                client = None
                dt = datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')
                try:
                    async with aioftp.Client().context(host, port, login, password) as client:
                        await client.download(ftp_path, f"Temp/{user_id}/{dt}/{file}", write_into=True)
                    with zipfile.ZipFile(f"Temp/{user_id}/{dt}/{file}") as z:
                        z.extract(xmlname, f"Temp/{user_id}/{dt}/")
                    sending_file = FSInputFile(f"Temp/{user_id}/{dt}/{xmlname}")
                    await bot.send_document(chat_id=user_id, document=sending_file, reply_to_message_id=message_id)
                    time.sleep(1)

                    os.unlink(f"Temp/{user_id}/{dt}/{file}")
                    os.unlink(f"Temp/{user_id}/{dt}/{xmlname}")
                    os.rmdir(f"Temp/{user_id}/{dt}/")
                    break
                except ConnectionResetError:
                    if os.path.exists(f"Temp/{user_id}/{dt}/{file}"):
                        os.unlink(f"Temp/{user_id}/{dt}/{file}")
                    if os.path.exists(f"Temp/{user_id}/{dt}/{xmlname}"):
                        os.unlink(f"Temp/{user_id}/{dt}/{xmlname}")
                        os.rmdir(f"Temp/{user_id}/{dt}/")
                finally:
                    if client:
                        client.close()


async def start_bot():
    logger.info('Запуск бота')
    pool = await asyncpg.create_pool(**credentials)
    scheduler = AsyncIOScheduler(timezone='Europe/Moscow')
    scheduler.add_job(main, args=[pool], trigger='interval', minutes=60, next_run_time=datetime.now())
    scheduler.start()
    queue = asyncio.Queue(maxsize=100)

    bot = Bot(token=TOKEN)
    dp = Dispatcher(pool=pool, queue=queue)

    # подключаем роутеры
    dp.include_router(users.router)
    dp.include_router(others.router)

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