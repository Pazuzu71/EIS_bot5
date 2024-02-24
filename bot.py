import asyncio
import re
import os
import zipfile
import time
from datetime import datetime


import asyncpg
import aioftp
from aiogram import Bot, Dispatcher
from aiogram import F
from aiogram.filters import CommandStart
from aiogram.types import Message, CallbackQuery, FSInputFile
from apscheduler.schedulers.asyncio import AsyncIOScheduler


from log.funcs import create_logger
from sql.funcs import get_psql_data, find_psql_document_id
from config import TOKEN, credentials, host, port, login, password
from app import main
from keyboards.funcs import kb_creator


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
    dp = Dispatcher()

    @dp.message(CommandStart())
    async def start(msg: Message):
        logger.info('это старт хэндлер')
        await msg.answer('это эхо')

    @dp.message(lambda msg: re.fullmatch(r'\d{19}', msg.text))
    async def get_over_here(msg: Message):
        logger.info(f'Перехвачено хэнлером, определяющим номер ЕИС 19 цифр: {msg.text}')
        documents = await find_psql_document_id(pool, msg.text)
        if not documents:
            await msg.reply(f'В базе нет информации по плану-графику с реестровым номером {msg.text}')
        else:
            notifications, protocols, contracts, contract_procedures = [], [], [], []
            for document in documents:
                xmlname: str = document[2]
                match xmlname:
                    case s if s.startswith('epNotification'):
                        notifications.append(document)
                    case s if s.startswith('epProtocol'):
                        protocols.append(document)
                    case s if s.startswith('contract_'):
                        contracts.append(document)
                    case s if s.startswith('contractProcedure_'):
                        contract_procedures.append(document)
            # Извещения
            if notifications:
                kb = kb_creator(notifications)
                await msg.reply(text=f'Извещения: {msg.text}', reply_markup=kb)
            # Протоколы
            if protocols:
                kb = kb_creator(protocols)
                await msg.reply(text=f'Протоколы: {msg.text}', reply_markup=kb)
            # СоК
            if contracts:
                kb = kb_creator(contracts)
                await msg.reply(text=f'Сведения о контракте (СоК): {msg.text}', reply_markup=kb)
            # СоИ
            if contract_procedures:
                kb = kb_creator(contract_procedures)
                await msg.reply(text=f'Сведения об исполнении (СоИ): {msg.text}', reply_markup=kb)

    @dp.message(lambda msg: re.fullmatch(r'\d{18}', msg.text))
    async def get_over_here(msg: Message):
        logger.info(f'Перехвачено хэнлером, определяющим номер ЕИС 18 цифр: {msg.text}')
        documents = await find_psql_document_id(pool, msg.text)
        if not documents:
            await msg.reply(f'В базе нет информации по плану-графику с реестровым номером {msg.text}')
        else:
            kb = kb_creator(documents)
            await msg.reply(text=msg.text, reply_markup=kb)

    @dp.message(lambda msg: re.fullmatch(r'\d{23}', msg.text))
    async def get_over_here(msg: Message):
        logger.info(f'Перехвачено хэнлером, определяющим номер ЕИС 19 цифр: {msg.text}')
        documents = await find_psql_document_id(pool, msg.text)
        if not documents:
            await msg.reply(f'В базе нет информации по проекту контракта с реестровым номером {msg.text}')
        if len(documents) == 1:
            xml_id = documents[0][1]
            ftp_path, xmlname = await get_psql_data(pool, int(xml_id))
            await queue.put((msg.from_user.id, msg.message_id, ftp_path, xmlname))
        else:
            kb = kb_creator(documents)
            await msg.reply(text=msg.text, reply_markup=kb)

    @dp.callback_query(F.data.startswith('document_'))
    async def get_document(callback: CallbackQuery):
        xml_id = callback.data.split('_')[-1]
        await callback.answer(text=f'id файла в базе {xml_id}')
        ftp_path, xmlname = await get_psql_data(pool, int(xml_id))
        await queue.put((callback.from_user.id, callback.message.message_id, ftp_path, xmlname))

    @dp.message()
    async def echo(msg: Message):
        logger.info('это эхо хэндлер')
        await msg.reply(msg.text)

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