import asyncio
import os
import re
import zipfile
from datetime import datetime


import aioftp
import aiosqlite
from bs4 import BeautifulSoup


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


async def insert_event(db, event_data):
    """Insert a new event into the files table"""
    sql = """INSERT INTO files 
              (ftppath, modify)  
              VALUES (?, ?)"""

    await db.execute(sql, (
       event_data['ftppath'],
       event_data['modify']
    ))
    await db.commit()


async def get_data(ftppath: str):
    file = ftppath.split('/')[-1]
    async with aioftp.Client.context(host, port, login, password) as client:
        print(f"Downloading file {file}...")
        await client.download(ftppath, f"Temp/{file}", write_into=True)
        print(f"Finished downloading file {file} into Temp/{file}")
        if file == 'contract_Tulskaja_obl_2024011200_2024011300_001.xml.zip':
            z = zipfile.ZipFile(f'Temp//{file}', 'r')
            for item in z.namelist():
                if item.endswith('.xml'):
                    print(f'Extract {item} from {ftppath}')
                    z.extract(item, 'Temp')
                with open(f'Temp//{item}') as f:
                    src = f.read()
                    print(re.search(r'(?<=<regNum>)\d{19}(?=</regNum>)', src)[0],
                          re.search(r'(?<=<publishDate>).+(?=</publishDate>)', src)[0])
                os.unlink(f'Temp//{item}')
            z.close()


async def get_ftp_list():
    async with aioftp.Client.context(host, port, login, password) as client:
        x = await client.list('/fcs_regions/Tulskaja_obl/contracts/currMonth', recursive=False)
    for path, info in x:
        # print(info)
        if info['size'] != '22':
            # print(info)
            res.append({
                'ftppath': str(path),
                'modify': datetime.strptime(info['modify'], '%Y%m%d%H%M%S').isoformat(sep='T', timespec='auto')})


async def main():
    async with aiosqlite.connect('sqlite.db') as db:
        await db.execute(TABLE_FILES)
    tasks = [
        asyncio.create_task(get_ftp_list())
    ]
    await asyncio.wait(tasks)
    # for p in res:
    #     print
    tasks = [
        asyncio.create_task(get_data(dct['ftppath'])) for dct in res
    ]
    await asyncio.wait(tasks)

    async with aiosqlite.connect('sqlite.db') as db:
        for event_data in res:
            await insert_event(db, event_data)


if __name__ == '__main__':
    host, port, login, password = 'ftp.zakupki.gov.ru', 21, 'free', 'free'
    res = []
    if not os.path.exists('Temp'):
        os.mkdir('Temp')
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print('Ошибка, останов бота!')
