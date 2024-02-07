import asyncio


from aiogram import Bot, Dispatcher
from aiogram.filters import Command, CommandStart
from aiogram.types import Message


from config import TOKEN


bot = Bot(token=TOKEN)
dp = Dispatcher()


@dp.message(CommandStart())
async def start(msg: Message):
    await msg.answer('это эхо')


@dp.message()
async def echo(msg: Message, bot: Bot):
    await bot.send_message(chat_id=msg.chat.id, text=msg.text)


if __name__ == '__main__':
    dp.run_polling(bot)

