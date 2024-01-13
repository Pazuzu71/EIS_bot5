import asyncio
import os
import re
import zipfile
from datetime import datetime


import aioftp
import aiosqlite
import pytz


host, port, login, password = 'ftp.zakupki.gov.ru', 21, 'free', 'free'
links = []

TABLE_FILES = '''create table if not exists files
    (
        id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        ftppath TEXT,
        modify TEXT,
        eisdocno TEXT,
        eispublicationdate TEXT,
        xmlname TEXT,
        creationdate TEXT,
        enddate TEXT
    )'''


async def insert_event(db, row):
    """Insert a new event into the files table"""
    sql = """INSERT INTO files 
              (ftppath, modify, eisdocno, eispublicationdate, xmlname, creationdate)  
              VALUES (?, ?, ?, ?, ?, ?)"""

    await db.execute(sql, (
        row['ftp_path'],
        row['modify'],
        row['eisdocno'],
        row['eispublicationdate'],
        row['xmlname'],
        datetime.now(pytz.timezone('Europe/Moscow')).isoformat(sep='T', timespec='auto')
    ))
    await db.commit()


async def get_data(ftp_path: str, modify: str):
    file = ftp_path.split('/')[-1]
    # client = aioftp.Client()
    # await client.connect(host=host)
    # await client.login(user='free', password='free')
    # print(f"Downloading file {file}...")
    # await client.download(ftp_path, f"Temp/{file}", write_into=True)
    # print(f"Finished downloading file {file} into Temp/{file}")
    # await client.quit()
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
        z = zipfile.ZipFile(f'Temp//{file}', 'r')
        for item in z.namelist():
            if item.endswith('.xml'):
                print(f'Extract {item} from {ftp_path}')
                z.extract(item, 'Temp')
                with open(f'Temp//{item}') as f:
                    src = f.read()
                    print(re.search(r'(?<=<regNum>)\d{19}(?=</regNum>)', src)[0],
                          re.search(r'(?<=<publishDate>).+(?=</publishDate>)', src)[0])
                    event_data.append({
                        'ftp_path': ftp_path,
                        'modify': modify,
                        'eisdocno': re.search(r'(?<=<regNum>)\d{19}(?=</regNum>)', src)[0],
                        'eispublicationdate': re.search(r'(?<=<publishDate>).+(?=</publishDate>)', src)[0],
                        'xmlname': item
                    })
                os.unlink(f'Temp//{item}')
        z.close()
    async with aiosqlite.connect('sqlite.db') as db:
        for row in event_data:
            await insert_event(db, row)


async def get_ftp_list():
    async with aioftp.Client.context(host, port, login, password) as client:
        x = await client.list('/fcs_regions/Tulskaja_obl/contracts/currMonth', recursive=False)
    for path, info in x:
        print(path, info)
        if info['size'] != '22':
            # print(info)
            links.append(
                (str(path),
                 datetime.strptime(info['modify'], '%Y%m%d%H%M%S').astimezone(pytz.timezone('Europe/Moscow')).isoformat(sep='T', timespec='auto'))
            )

            # links.append(str(path))


async def main():
    async with aiosqlite.connect('sqlite.db') as db:
        await db.execute(TABLE_FILES)
    tasks = [
        asyncio.create_task(get_ftp_list())
    ]
    await asyncio.gather(*tasks)
    tasks = [
        asyncio.create_task(get_data(ftp_path, modify)) for ftp_path, modify in links
    ]
    await asyncio.gather(*tasks)
    # print(event_data)
    # async with aiosqlite.connect('sqlite.db') as db:
    #     for event_data in event_data:
    #         await insert_event(db, event_data)


if __name__ == '__main__':

    if not os.path.exists('Temp'):
        os.mkdir('Temp')
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print('Ошибка, останов бота!')
