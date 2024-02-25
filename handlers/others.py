from aiogram.types import Message
from aiogram.filters import CommandStart
from aiogram import Router


from log.funcs import create_logger


logger = create_logger(__name__)

router: Router = Router()


@router.message(CommandStart())
async def start(msg: Message):
    logger.info('это старт хэндлер')
    await msg.answer('это эхо')


@router.message()
async def echo(msg: Message):
    logger.info('это эхо хэндлер')
    await msg.reply(msg.text)
