import asyncio
import os
import re
import zipfile
import time
from datetime import datetime


import aioftp
import asyncpg
from asyncpg.connection import Connection


host, port, login, password = 'ftp.zakupki.gov.ru', 21, 'free', 'free'
links = []
folders = [
    '/fcs_regions/Tulskaja_obl/contracts/currMonth',
    # '/fcs_regions/Tulskaja_obl/contracts/prevMonth'
]


async def create_psql_db():
    try:
        conn = await asyncpg.connect(host='127.0.0.1', user='postgres', password='123', database='psql_db')
        await conn.close()
        print('База постгрес существует')
    except asyncpg.InvalidCatalogNameError:
        print('База постгрес не существует и будет создана')
        conn = await asyncpg.connect(host='127.0.0.1', user='postgres', password='123', database='template1')
        await conn.execute(
            'CREATE DATABASE psql_db OWNER postgres'
        )
        await conn.close()
        print('База постгрес создана')


async def create_psql_tables():
    conn: Connection = await asyncpg.connect(host='127.0.0.1', user='postgres', password='123', database='psql_db')
    await conn.execute('''
            create table if not exists zip
            (
                zip_id SERIAL PRIMARY KEY,
                ftp_path VARCHAR,
                modify VARCHAR,
                creationdate timestamp,
                enddate timestamp)
    ''')
    await conn.execute('''
                create table if not exists xml
                (
                zip_id INTEGER REFERENCES zip (zip_id) ON DELETE CASCADE,
                xml_id SERIAL PRIMARY KEY,
                eisdocno VARCHAR,
                eispublicationdate timestamp with time zone,
                xmlname VARCHAR)
        ''')
    await conn.close()


async def insert_psql_zip(ftp_path: str, modify: str):

    conn: Connection = await asyncpg.connect(host='127.0.0.1', user='postgres', password='123', database='psql_db')
    await conn.execute('''INSERT INTO zip (ftp_path, modify, creationdate) VALUES ($1, $2, $3)''',
                       ftp_path, modify, datetime.now()
                       )
    await conn.close()


async def insert_psql_xml(row):

    conn: Connection = await asyncpg.connect(host='127.0.0.1', user='postgres', password='123', database='psql_db')
    zip_id = await conn.fetchval("""SELECT * FROM zip WHERE ftp_path = $1 AND modify = $2 AND enddate IS NULL""",
                                 row['ftp_path'], row['modify'], column=0)
    # print('zip_id', zip_id)
    await conn.execute("""INSERT INTO xml (zip_id, eisdocno, eispublicationdate, xmlname) VALUES ($1, $2, $3, $4)""",
                       zip_id, row['eisdocno'], row['eispublicationdate'], row['xmlname'])
    await conn.close()


async def exist_in_psql_db(ftp_path: str, modify: str):

    conn: Connection = await asyncpg.connect(host='127.0.0.1', user='postgres', password='123', database='psql_db')
    is_exist = await conn.fetch("""SELECT * FROM zip WHERE ftp_path = $1 AND modify = $2 AND enddate IS NULL""",
                                ftp_path, modify)
    await conn.close()
    return is_exist


async def get_data(ftp_path: str, modify: str, semaphore):
    file = ftp_path.split('/')[-1]
    async with semaphore:
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
    # if file == 'contract_Tulskaja_obl_2024011200_2024011300_001.xml.zip':
    with zipfile.ZipFile(f'Temp//{file}', 'r') as z:
        for item in z.namelist():
            if item.endswith('.xml') and not any(
                    [item.startswith('contractAvailableForElAct'), item.startswith('contractProcedureCancel')]):
                print(f'Extract {item} from {file}')
                z.extract(item, 'Temp')
                with open(f'Temp//{item}') as f:
                    src = f.read()
                    if item.startswith('contract'):
                        eisdocno = re.search(r'(?<=<regNum>)\d{19}(?=</regNum>)', src)[0]
                        eispublicationdate = re.search(r'(?<=<publishDate>).+(?=</publishDate>)', src)[0]
                    # try:
                    #     eispublicationdate = re.search(r'(?<=<publishDate>).+(?=</publishDate>)', src)[0]
                    # except Exception as e:
                    #     print(e, item)
                    # print(eisdocno, eispublicationdate)
                    event_data.append({
                        'ftp_path': ftp_path,
                        'modify': modify,
                        'eisdocno': eisdocno,
                        'eispublicationdate': datetime.fromisoformat(eispublicationdate),
                        'xmlname': item
                    })
                os.unlink(f'Temp//{item}')
    os.unlink(f'Temp//{file}')
    await insert_psql_zip(ftp_path, modify)
    for row in event_data:
        await insert_psql_xml(row)


async def get_ftp_list(folder: str, semaphore):
    async with semaphore:
        async with aioftp.Client.context(host, port, login, password) as client:
            ftp_list = await client.list(folder, recursive=False)
        for path, info in ftp_list:
            exist_in_db = await exist_in_psql_db(str(path), info['modify'])
            if info['size'] != '22' and not exist_in_db:
                links.append(
                    (str(path), info['modify'])
                )


async def main():
    if not os.path.exists('Temp'):
        os.mkdir('Temp')

    await create_psql_db()
    await create_psql_tables()

    semaphore = asyncio.Semaphore(80)
    # semaphore_psql = asyncio.Semaphore(75)
    tasks = [
        asyncio.create_task(get_ftp_list(folder, semaphore)) for folder in folders
    ]
    await asyncio.gather(*tasks)
    tasks = [
        asyncio.create_task(get_data(ftp_path, modify, semaphore)) for ftp_path, modify in links
    ]
    await asyncio.gather(*tasks)


if __name__ == '__main__':
    start = time.time()
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print('Ошибка, останов бота!')
    print('Время работы: ', divmod(time.time() - start, 60))
