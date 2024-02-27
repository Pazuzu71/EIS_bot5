import pytz
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import InlineKeyboardButton


def kb_creator(documents):
    documents = sorted([
        (eispublicationdate.astimezone(tz=pytz.timezone('Europe/Moscow')), xml_id)
        for eispublicationdate, xml_id, xmlname in documents
    ], reverse=True)
    # print(documents)
    buttons = [
        InlineKeyboardButton(text=document[0].strftime('%Y-%m-%d %H:%M'),
                             callback_data=f'document_{document[1]}') for document in documents
    ]
    kb_builder = InlineKeyboardBuilder()
    kb_builder.row(width=3, *buttons)
    return kb_builder.as_markup()
