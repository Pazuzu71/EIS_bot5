import asyncio
import os
import re
import zipfile
from datetime import datetime

import aioftp
import aiosqlite
import asyncpg
import pytz

host, port, login, password = 'ftp.zakupki.gov.ru', 21, 'free', 'free'
links = []
folders = [
    '/fcs_regions/Tulskaja_obl/contracts/currMonth',
    # '/fcs_regions/Tulskaja_obl/contracts/prevMonth'
]


async def create_db():
    try:
        conn = await asyncpg.connect(host='127.0.0.1', user='postgres', password='123', database='psql_db')
        await conn.close()
        print('База постгрес существует')
    except asyncpg.InvalidCatalogNameError:
        print('База постгрес не существует')
        conn = await asyncpg.connect(host='127.0.0.1', user='postgres', password='123', database='template1')
        await conn.execute(
                'CREATE DATABASE psql_db OWNER postgres'
            )
        await conn.close()
        print('База постгрес создана')


async def create_tables():
    async with aiosqlite.connect('sqlite.db') as db:
        await db.executescript("""create table if not exists files
            (
                id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                ftppath TEXT,
                modify TEXT,
                eisdocno TEXT,
                eispublicationdate TEXT,
                xmlname TEXT,
                creationdate TEXT,
                enddate TEXT
            )"""
                               )
        await db.commit()


async def insert_event(row):
    """Insert a new event into the files table"""
    async with aiosqlite.connect('sqlite.db') as db:
        sql = """INSERT INTO files 
                  (ftppath, modify, eisdocno, eispublicationdate, xmlname, creationdate)  
                  VALUES (?, ?, ?, ?, ?, ?)"""

        await db.execute(sql,
                         (
                             row['ftp_path'],
                             row['modify'],
                             row['eisdocno'],
                             row['eispublicationdate'],
                             row['xmlname'],
                             datetime.now(pytz.timezone('Europe/Moscow')).isoformat(sep='T', timespec='auto')
                         ))
        await db.commit()


async def not_exist_in_db(ftp_path: str, modify: str):
    async with aiosqlite.connect('sqlite.db') as db:
        async with db.execute("""SELECT COUNT(*) FROM files WHERE ftppath = ? AND modify = ?""",
                              (ftp_path, modify)) as cursor:
            res = await cursor.fetchone()
    return not bool(res[0])


async def get_data(ftp_path: str, modify: str, sem):
    file = ftp_path.split('/')[-1]
    async with sem:
        while True:
            try:
                async with aioftp.Client.context(host, port, login, password) as client:
                    print(f"Downloading file {file}...")
                    await client.download(ftp_path, f"Temp/{file}", write_into=True)
                    print(f"Finished downloading file {file} into Temp/{file}")
                break
            except ConnectionResetError:
                print('ConnectionResetError')
                pass
    event_data = []
    if file == 'contract_Tulskaja_obl_2024011200_2024011300_001.xml.zip':
        with zipfile.ZipFile(f'Temp//{file}', 'r') as z:
            for item in z.namelist():
                if item.endswith('.xml') and not any(
                        [item.startswith('contractAvailableForElAct'), item.startswith('contractProcedureCancel')]):
                    print(f'Extract {item} from {ftp_path}')
                    z.extract(item, 'Temp')
                    with open(f'Temp//{item}') as f:
                        src = f.read()
                        if item.startswith('contract'):
                            eisdocno = re.search(r'(?<=<regNum>)\d{19}(?=</regNum>)', src)[0]
                        try:
                            eispublicationdate = re.search(r'(?<=<publishDate>).+(?=</publishDate>)', src)[0]
                        except Exception as e:
                            print(e, item)
                        print(eisdocno, eispublicationdate)
                        event_data.append({
                            'ftp_path': ftp_path,
                            'modify': modify,
                            'eisdocno': eisdocno,
                            'eispublicationdate': eispublicationdate,
                            'xmlname': item
                        })
                    os.unlink(f'Temp//{item}')
    for row in event_data:
        await insert_event(row)


async def get_ftp_list(folder: str, sem):
    async with sem:
        async with aioftp.Client.context(host, port, login, password) as client:
            x = await client.list(folder, recursive=False)
        for path, info in x:
            print(path, info)
            no_rows_in_db = await not_exist_in_db(str(path), info['modify'])
            print('Записей в базе нет:', no_rows_in_db)
            if info['size'] != '22' and no_rows_in_db:
                links.append(
                    (str(path), info['modify'])
                )


async def main():
    if not os.path.exists('Temp'):
        os.mkdir('Temp')

    await create_tables()
    await create_db()

    semaphore = asyncio.Semaphore(50)
    tasks = [
        asyncio.create_task(get_ftp_list(folder, semaphore)) for folder in folders
    ]
    await asyncio.gather(*tasks)
    tasks = [
        asyncio.create_task(get_data(ftp_path, modify, semaphore)) for ftp_path, modify in links
    ]
    await asyncio.gather(*tasks)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print('Ошибка, останов бота!')
