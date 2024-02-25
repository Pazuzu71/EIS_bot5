import re
from asyncio import Queue


from aiogram.types import Message, CallbackQuery
from aiogram import Router
from aiogram import F
from asyncpg.pool import Pool


from log.funcs import create_logger
from keyboards.funcs import kb_creator
from sql.funcs import find_psql_document_id, get_psql_data


logger = create_logger(__name__)

router: Router = Router()


@router.message(lambda msg: re.fullmatch(r'\d{19}', msg.text))
async def get_over_here(msg: Message, pool: Pool):
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


@router.message(lambda msg: re.fullmatch(r'\d{18}', msg.text))
async def get_over_here(msg: Message, pool: Pool):
    logger.info(f'Перехвачено хэнлером, определяющим номер ЕИС 18 цифр: {msg.text}')
    documents = await find_psql_document_id(pool, msg.text)
    if not documents:
        await msg.reply(f'В базе нет информации по плану-графику с реестровым номером {msg.text}')
    else:
        kb = kb_creator(documents)
        await msg.reply(text=msg.text, reply_markup=kb)


@router.message(lambda msg: re.fullmatch(r'\d{23}', msg.text))
async def get_over_here(msg: Message, pool: Pool, queue: Queue):
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


@router.callback_query(F.data.startswith('document_'))
async def get_document(callback: CallbackQuery, pool: Pool, queue: Queue):
    xml_id = callback.data.split('_')[-1]
    await callback.answer(text=f'id файла в базе {xml_id}')
    ftp_path, xmlname = await get_psql_data(pool, int(xml_id))
    await queue.put((callback.from_user.id, callback.message.message_id, ftp_path, xmlname))
