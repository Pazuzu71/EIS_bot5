import asyncio
import logging
import re
from datetime import datetime


from aiogram import Bot, Dispatcher
from aiogram.filters import CommandStart
from aiogram.types import Message
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncpg
from asyncpg.pool import Pool


from config import TOKEN, credentials
from app import main


# Настраиваем базовую конфигурацию логирования
logging.basicConfig(
    format='[%(asctime)s] #%(levelname)-8s %(filename)s: %(lineno)d - %(name)s:%(funcName)s - %(message)s',
    level=logging.DEBUG,
)
# Инициализируем логгер модуля
logger = logging.getLogger()
# Устанавливаем логгеру уровень `DEBUG`
logger.setLevel(logging.INFO)
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


async def start_bot():
    logger.debug('Запуск бота')
    bot = Bot(token=TOKEN)
    dp = Dispatcher()

    @dp.message(CommandStart())
    async def start(msg: Message):
        logger.info('это старт хэндлер')
        await msg.answer('это эхо')

    # @dp.message(lambda msg: msg.text == '111')
    @dp.message(lambda msg: re.fullmatch(r'\d{18}', msg.text))
    async def get_over_here(msg: Message):
        logger.info('Перехвачено хэнлером, определяющим номер ЕИС 18 цифр')
        await msg.answer('да, это тот номер, и дальше понеслось!')

    @dp.message()
    async def echo(msg: Message):
        logger.info('это эхо хэндлер')
        await msg.reply(msg.text)
        async with asyncpg.create_pool(**credentials) as pool:
            pass

    scheduler = AsyncIOScheduler(timezone='Europe/Moscow')
    scheduler.add_job(main, trigger='interval', minutes=60, next_run_time=datetime.now())
    scheduler.start()

    # очищаем очередь апдейтов, запускаем поулинг
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == '__main__':
    try:
        logger.info('запус мэйна 3 2 1..')
        asyncio.run(start_bot())
    except (KeyboardInterrupt, SystemExit):
        print('Ошибка, остановка бота!')
