from aiogram.types import Message
from aiogram.filters import CommandStart, Command
from aiogram import Router


from log.main_logger import create_logger


logger = create_logger(__name__)

router: Router = Router()


@router.message(CommandStart())
async def start(msg: Message):
    logger.info('это старт хэндлер')
    await msg.answer('это эхо')


@router.message(Command('help'))
async def start(msg: Message):
    logger.info('это запрос справки по работе бота')
    await msg.answer('Для поиска файла нужно ввести его реестровый номер в ЕИС. '
                     'Если файл будет найден, появится выбор даты публикации в ЕИС.')


@router.message()
async def echo(msg: Message):
    logger.info('это эхо хэндлер')
    await msg.reply(msg.text)
