import asyncio


import aioschedule
from aiogram import Bot, Dispatcher
from aiogram.filters import CommandStart
from aiogram.types import Message


from config import TOKEN
from app import main


async def start_scheduler():
    aioschedule.every().hour.at(":03").do(main)
    while True:
        await aioschedule.run_pending()
        # await main()
        await asyncio.sleep(1)


async def start_bot():
    bot = Bot(token=TOKEN)
    dp = Dispatcher()

    @dp.message(CommandStart())
    async def start(msg: Message):
        await msg.answer('это эхо')

    @dp.message()
    async def echo(msg: Message, bot: Bot):
        await bot.send_message(chat_id=msg.chat.id, text=msg.text)

    # очищаем очередь апдейтов, запускаем поулинг
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


async def main2():
    async with asyncio.TaskGroup() as tg:
        tg.create_task((start_bot()))
        tg.create_task(start_scheduler())


if __name__ == '__main__':
    # todo поправить линки, они должны собираться здесь
    # links = []
    try:
        asyncio.run(main2())
    except (KeyboardInterrupt, SystemExit):
        print('Ошибка, остановка бота!')
