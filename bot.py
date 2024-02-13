import asyncio
import logging
from datetime import datetime


import aioschedule
from aiogram import Bot, Dispatcher
from aiogram.filters import CommandStart
from aiogram.types import Message
from apscheduler.schedulers.asyncio import AsyncIOScheduler


from config import TOKEN, scheduler_time
from app import main


# Настраиваем базовую конфигурацию логирования
logging.basicConfig(
    format='[%(asctime)s] #%(levelname)-8s %(filename)s: %(lineno)d - %(name)s:%(funcName)s - %(message)s'
)
# Инициализируем логгер модуля
logger = logging.getLogger(__name__)
# Устанавливаем логгеру уровень `DEBUG`
logger.setLevel(logging.DEBUG)
# Инициализируем хэндлер, который будет писать логи в файл `error.log`
error_file = logging.FileHandler('error.log', 'w', encoding='utf-8')
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


# async def start_scheduler():
#     logger.debug('Запуск расписания')
#     aioschedule.every().hour.at(scheduler_time).do(main)
#     while True:
#         await aioschedule.run_pending()
#         # await main()
#         await asyncio.sleep(1)


async def start_bot():
    logger.debug('Запуск бота')
    bot = Bot(token=TOKEN)
    dp = Dispatcher()

    @dp.message(CommandStart())
    async def start(msg: Message):
        logger.debug('это старт хэндлер')
        await msg.answer('это эхо')

    @dp.message()
    async def echo(msg: Message):
        logger.debug('это эхо хэндлер')
        await msg.reply(msg.text)

    scheduler = AsyncIOScheduler(timezone='Europe/Moscow')
    scheduler.add_job(main, trigger='interval', minutes=60, next_run_time=datetime.now())
    scheduler.start()

    # очищаем очередь апдейтов, запускаем поулинг
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


# async def main2():
#     async with asyncio.TaskGroup() as tg:
#         tg.create_task((start_bot()))
#         tg.create_task(start_scheduler())
    # task1 = asyncio.create_task(start_bot())
    # task2 = asyncio.create_task(start_scheduler())
    # await asyncio.gather(task1, task2)


if __name__ == '__main__':
    try:
        logger.debug('запус мэйна 3 2 1..')
        asyncio.run(start_bot())
    except (KeyboardInterrupt, SystemExit):
        print('Ошибка, остановка бота!')
