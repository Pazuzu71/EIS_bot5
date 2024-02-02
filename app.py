import asyncio
import os
import re
import zipfile
import time
from datetime import datetime


import aioftp
import asyncpg
from asyncpg.pool import Pool

# TODO вынести все константы в отдельный файлы или в переменные окружения
host, port, login, password = 'ftp.zakupki.gov.ru', 21, 'free', 'free'
links = []
folders = [
    # '/fcs_regions/Tulskaja_obl/contracts',
    # '/fcs_regions/Tulskaja_obl/contracts/currMonth',
    # '/fcs_regions/Tulskaja_obl/contracts/prevMonth',
    '/fcs_regions/Tulskaja_obl/notifications',
    '/fcs_regions/Tulskaja_obl/notifications/currMonth',
    '/fcs_regions/Tulskaja_obl/notifications/prevMonth',
    # '/fcs_regions/Tulskaja_obl/plangraphs2020,
    # '/fcs_regions/Tulskaja_obl/plangraphs2020/currMonth',
    # '/fcs_regions/Tulskaja_obl/plangraphs2020/prevMonth',
    # '/fcs_regions/Tulskaja_obl/protocols',
    # '/fcs_regions/Tulskaja_obl/protocols/currMonth',
    # '/fcs_regions/Tulskaja_obl/protocols/prevMonth',
    # '/fcs_regions/Tulskaja_obl/contractprojects',
    # '/fcs_regions/Tulskaja_obl/contractprojects/currMonth',
    # '/fcs_regions/Tulskaja_obl/contractprojects/prevMonth',
]
credentials = dict(
    host='127.0.0.1',
    port=5432,
    user='db_user',
    password='myPassword',
    database='psql_db',
    min_size=5,
    max_size=75
)


async def create_psql_tables(pool: Pool):
    conn = await pool.acquire()
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
    await pool.release(conn)


async def insert_psql_zip(pool: Pool, ftp_path: str, modify: str):
    """Добавление записи в таблицу zip"""
    conn = await pool.acquire()
    await conn.execute('''INSERT INTO zip (ftp_path, modify, creationdate) VALUES ($1, $2, $3)''',
                       ftp_path, modify, datetime.now()
                       )
    await pool.release(conn)


async def insert_psql_xml(pool: Pool, row: dict):
    """Добавление записи в таблицу xml"""
    # TODO проверить конвертацию поля eispublicationdate в зонах времени
    conn = await pool.acquire()
    zip_id = await conn.fetchval("""SELECT * FROM zip WHERE ftp_path = $1 AND modify = $2 AND enddate IS NULL""",
                                 row['ftp_path'], row['modify'], column=0)
    await conn.execute("""INSERT INTO xml (zip_id, eisdocno, eispublicationdate, xmlname) VALUES ($1, $2, $3, $4)""",
                       zip_id, row['eisdocno'], row['eispublicationdate'], row['xmlname'])
    await pool.release(conn)


async def exist_in_psql_db(pool: Pool, ftp_path: str, modify: str):
    conn = await pool.acquire()
    is_exist = await conn.fetch("""SELECT * FROM zip WHERE ftp_path = $1 AND modify = $2 AND enddate IS NULL""",
                                ftp_path, modify)
    await pool.release(conn)
    return is_exist


async def get_psql_paths(pool: Pool):
    async with pool.acquire() as conn:
        return await conn.fetch("""SELECT ftp_path, modify FROM zip WHERE enddate IS NULL""")


async def get_data(pool: Pool, ftp_path: str, modify: str, semaphore):
    file = ftp_path.split('/')[-1]
    if file not in ['currMonth', 'prevMonth']:
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
        # if file == 'notification_Tulskaja_obl_2024010100_2024010200_001.xml.zip':
        with zipfile.ZipFile(f'Temp//{file}', 'r') as z:
            for item in z.namelist():
                if item.endswith('.xml') and any(
                        [item.startswith('contractProcedure_'), item.startswith('contract_'),
                         all([item.startswith('epNotification'), not item.startswith('epNotificationCancel')]),
                         item.startswith('tenderPlan2020_'), item.startswith('epProtocol'),
                         item.startswith('cpContractSign')]):
                    print(f'Extract {item} from {file}')
                    z.extract(item, 'Temp')
                    with open(f'Temp//{item}') as f:
                        src = f.read()
                        if item.startswith('contract'):
                            try:
                                eisdocno = re.search(r'(?<=<regNum>)\d{19}(?=</regNum>)', src)[0]
                                eispublicationdate = re.search(r'(?<=<publishDate>).+(?=</publishDate>)', src)[0]
                            except Exception as e:
                                print(e, item)
                        if any([item.startswith('epNotification'), item.startswith('epProtocol')]):
                            try:
                                eisdocno = re.search(r'(?<=<ns9:purchaseNumber>)\d{19}(?=</ns9:purchaseNumber>)', src)[0]
                                eispublicationdate = re.search(r'(?<=<ns9:publishDTInEIS>).+(?=</ns9:publishDTInEIS>)', src)[0]
                            except Exception as e:
                                print(e, item)
                        if item.startswith('tenderPlan2020'):
                            try:
                                eisdocno = re.search(r'(?<=<ns5:planNumber>)\d{18}(?=</ns5:planNumber>)', src)[0]
                                eispublicationdate = re.search(r'(?<=<ns5:publishDate>).+(?=</ns5:publishDate>)', src)[0]
                            except Exception as e:
                                print(e, item)
                        if item.startswith('cpContractSign'):
                            try:
                                common_info = re.search(r'(?<=<ns7:commonInfo>).+(?=</ns7:commonInfo>)', src, flags=re.DOTALL)[0]
                                eisdocno = re.search(r'(?<=<ns7:number>)\d{23}(?=</ns7:number>)', common_info)[0]
                                eispublicationdate = re.search(r'(?<=<ns7:publishDTInEIS>).+(?=</ns7:publishDTInEIS>)', common_info)[0]
                            except Exception as e:
                                print(e, item)

                        try:
                            event_data.append({
                                'ftp_path': ftp_path,
                                'modify': modify,
                                'eisdocno': eisdocno,
                                'eispublicationdate': datetime.fromisoformat(eispublicationdate),
                                'xmlname': item
                            })
                        except Exception as e:
                            print(e)

                    os.unlink(f'Temp//{item}')
        os.unlink(f'Temp//{file}')
        await insert_psql_zip(pool, ftp_path, modify)
        if event_data:
            for row in event_data:
                await insert_psql_xml(pool, row)


async def get_ftp_list(pool: Pool, folder: str, semaphore):
    async with semaphore:
        async with aioftp.Client.context(host, port, login, password) as client:
            ftp_list = await client.list(folder, recursive=False)
        for path, info in ftp_list:
            exist_in_db = await exist_in_psql_db(pool, str(path), info['modify'])
            if info['size'] != '22' and not exist_in_db:
                if any(sub_path in str(path) for sub_path in ['currMonth', 'prevMonth', '2022', '2023']):
                    links.append(
                        (str(path), info['modify'])
                    )


async def set_psql_enddate(pool: Pool, ftp_path: str, semaphore):
    pass


async def exist_on_ftp(pool: Pool, ftp_path: str, modify: str, semaphore):
    async with semaphore:
        while True:
            try:
                async with aioftp.Client.context(host, port, login, password) as client:
                    try:
                        info = await client.stat(ftp_path)
                        print(info)
                        if info['modify'] != modify:
                            print('Здесь надо будет обновить в базе enddate, отличается дата модификации')
                            await set_psql_enddate(pool, ftp_path, semaphore)
                    except aioftp.errors.StatusCodeError:
                        print(aioftp.errors.StatusCodeError, ftp_path)
                        print('Здесь надо будет обновить в базе enddate')
                        await set_psql_enddate(pool, ftp_path, semaphore)
                break
            except ConnectionResetError:
                print('ConnectionResetError')
                pass


async def main():
    if not os.path.exists('Temp'):
        os.mkdir('Temp')
    semaphore = asyncio.Semaphore(50)
    async with asyncpg.create_pool(**credentials) as pool:
        await create_psql_tables(pool)
        psql_list = await get_psql_paths(pool)
        tasks = [
            asyncio.create_task(exist_on_ftp(pool, ftp_path, modify, semaphore)) for ftp_path, modify in psql_list
        ]
        await asyncio.gather(*tasks)
        tasks = [
            asyncio.create_task(get_ftp_list(pool, folder, semaphore)) for folder in folders
        ]
        await asyncio.gather(*tasks)
        tasks = [
            asyncio.create_task(get_data(pool, ftp_path, modify, semaphore)) for ftp_path, modify in links
        ]
        await asyncio.gather(*tasks)


if __name__ == '__main__':
    start = time.time()
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print('Ошибка, останов бота!')
    print('Время работы: ', divmod(time.time() - start, 60))
