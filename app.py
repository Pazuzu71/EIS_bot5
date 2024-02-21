import asyncio
import os
import re
import zipfile
import time
import logging
from datetime import datetime


import aioftp
from asyncpg.pool import Pool


from config import semaphore_value, host, port, login, password


# Настраиваем базовую конфигурацию логирования
logging.basicConfig(
    format='[%(asctime)s] #%(levelname)-8s %(filename)s: %(lineno)d - %(name)s:%(funcName)s - %(message)s',
    level=logging.INFO,
)
# Инициализируем логгер модуля
logger = logging.getLogger(__name__)
# Устанавливаем логгеру уровень `DEBUG`
logger.setLevel(logging.DEBUG)
# Инициализируем хэндлер, который будет писать логи в файл `error.log`
error_file = logging.FileHandler('error.log', 'a', encoding='utf-8')
# Устанавливаем хэндлеру уровень `DEBUG`
error_file.setLevel(logging.DEBUG)
# Инициализируем форматтер
formatter_1 = logging.Formatter(
    fmt='[%(asctime)s] #%(levelname)-8s %(filename)s:'
        '%(lineno)d - %(name)s:%(funcName)s - %(message)s'
)
# Определяем форматирование логов в хэндлере
error_file.setFormatter(formatter_1)
# Добавляем хэндлер в логгер
logger.addHandler(error_file)



# TODO вынести все константы в отдельный файлы или в переменные окружения


folders = [
    '/fcs_regions/Tulskaja_obl/contracts',
    '/fcs_regions/Tulskaja_obl/contracts/currMonth',
    '/fcs_regions/Tulskaja_obl/contracts/prevMonth',
    '/fcs_regions/Tulskaja_obl/notifications',
    '/fcs_regions/Tulskaja_obl/notifications/currMonth',
    '/fcs_regions/Tulskaja_obl/notifications/prevMonth',
    '/fcs_regions/Tulskaja_obl/plangraphs2020',
    '/fcs_regions/Tulskaja_obl/plangraphs2020/currMonth',
    '/fcs_regions/Tulskaja_obl/plangraphs2020/prevMonth',
    '/fcs_regions/Tulskaja_obl/protocols',
    '/fcs_regions/Tulskaja_obl/protocols/currMonth',
    '/fcs_regions/Tulskaja_obl/protocols/prevMonth',
    '/fcs_regions/Tulskaja_obl/contractprojects',
    '/fcs_regions/Tulskaja_obl/contractprojects/currMonth',
    '/fcs_regions/Tulskaja_obl/contractprojects/prevMonth',
]


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
    conn = await pool.acquire()
    # todo что будет, если нвйдутся две строчки без даты
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
    conn = None
    try:
        async with pool.acquire() as conn:
            psql_list = await conn.fetch("""SELECT ftp_path, modify FROM zip WHERE enddate IS NULL""")
        return psql_list
    except Exception as e:
        logger.exception(e)
    finally:
        if conn:
            await pool.release(conn)


async def insert_data(pool: Pool, file: str, ftp_path: str, modify: str, semaphore):
    event_data = []
    with zipfile.ZipFile(f'Temp//{file}', 'r') as z:
        for item in z.namelist():
            if item.endswith('.xml') and any(
                    [item.startswith('contractProcedure_'), item.startswith('contract_'),
                     all([item.startswith('epNotification'), not item.startswith('epNotificationCancel')]),
                     item.startswith('tenderPlan2020_'),
                     item.startswith('epProtocol'),
                     # all([item.startswith('epProtocol'), not item.startswith('epProtocolCancel'), not item.startswith('epProtocolDeviation')]),
                     item.startswith('cpContractSign')]):
                # print(f'Extract {item} from {file}')
                z.extract(item, 'Temp')
                with open(f'Temp//{item}') as f:
                    src = f.read()
                    if item.startswith('contract'):
                        try:
                            eisdocno = re.search(r'(?<=<regNum>)\d{19}(?=</regNum>)', src)[0]
                            eispublicationdate = re.search(r'(?<=<publishDate>).+(?=</publishDate>)', src)[0]
                        except Exception as e:
                            logger.exception(item, e)
                            continue
                    if any([item.startswith('epNotification'), item.startswith('epProtocol')]):
                        try:
                            eisdocno = re.search(r'(?<=<ns9:purchaseNumber>)\d{19}(?=</ns9:purchaseNumber>)', src)[0]
                            eispublicationdate = re.search(r'(?<=<ns9:publishDTInEIS>).+(?=</ns9:publishDTInEIS>)', src)[0]
                        except Exception as e:
                            logger.exception(item, e)
                            continue
                    if item.startswith('tenderPlan2020'):
                        try:
                            eisdocno = re.search(r'(?<=<ns5:planNumber>)\d{18}(?=</ns5:planNumber>)', src)[0]
                            eispublicationdate = re.search(r'(?<=<ns5:publishDate>).+(?=</ns5:publishDate>)', src)[0]
                        except Exception as e:
                            logger.exception(item, e)
                            continue
                    if item.startswith('cpContractSign'):
                        try:
                            common_info = re.search(r'(?<=<ns7:commonInfo>).+(?=</ns7:commonInfo>)', src, flags=re.DOTALL)[0]
                            eisdocno = re.search(r'(?<=<ns7:number>)\d{23}(?=</ns7:number>)', common_info)[0]
                            eispublicationdate = re.search(r'(?<=<ns7:publishDTInEIS>).+(?=</ns7:publishDTInEIS>)', common_info)[0]
                        except Exception as e:
                            logger.exception(item, e)
                            continue
                    try:
                        event_data.append({
                            'ftp_path': ftp_path,
                            'modify': modify,
                            'eisdocno': eisdocno,
                            'eispublicationdate': datetime.fromisoformat(eispublicationdate),
                            'xmlname': item
                        })
                    except Exception as e:
                        logger.exception(e)

                os.unlink(f'Temp//{item}')
    os.unlink(f'Temp//{file}')
    # async with semaphore:
    await insert_psql_zip(pool, ftp_path, modify)
    if event_data:
        for row in event_data:
            await insert_psql_xml(pool, row)


async def get_data(pool: Pool, file: str, ftp_path: str, modify: str, semaphore):
    async with semaphore:
        while True:
            try:
                async with aioftp.Client().context(host, port, login, password) as client:
                    # print(f"Downloading file {file}...")
                    await client.download(ftp_path, f"Temp/{file}", write_into=True)
                    # print(f"Finished downloading file {file} into Temp/{file}")
                break
            except ConnectionResetError:
                time.sleep(1)
                # print(f'Алярм!!! {file}', os.path.exists(f"Temp/{file}"))
                if os.path.exists(f"Temp/{file}"):
                    os.unlink(f"Temp/{file}")
                # print(f'ConnectionResetError при получении {file} с фтп')
        await insert_data(pool, file, ftp_path, modify, semaphore)


async def get_ftp_list(pool: Pool, links: list, folder: str, semaphore):
    async with semaphore:
        while True:
            try:
                async with aioftp.Client.context(host, port, login, password) as client:
                    ftp_list = await client.list(folder, recursive=False)
                break
            except ConnectionResetError:
                logger.exception(ConnectionResetError)

        for path, info in ftp_list:
            exist_in_db = await exist_in_psql_db(pool, str(path), info['modify'])
            if info['size'] != '22' and not exist_in_db and str(path).endswith('.zip'):
                # TODO текущий и прошлый месяцы будут всегда, года вынести в отдельный параметр
                if any(sub_path in str(path) for sub_path in ['currMonth', 'prevMonth'] + ['2022', '2023']):
                    links.append(
                        (str(path).split('/')[-1], str(path), info['modify'], 1)
                    )
            elif info['size'] != '22' and str(path).endswith('.zip'):
                # TODO текущий и прошлый месяцы будут всегда, года вынести в отдельный параметр
                if any(sub_path in str(path) for sub_path in ['currMonth', 'prevMonth'] + ['2022', '2023']):
                    links.append(
                        (str(path).split('/')[-1], str(path), info['modify'], 0)
                    )


async def set_psql_enddate(pool: Pool, ftp_path: str):
    conn = None
    try:
        async with pool.acquire() as conn:
            await conn.execute("""UPDATE zip set enddate = $1 WHERE ftp_path = $2""", datetime.now(), ftp_path)
    except Exception as e:
        logger.exception(e)
    finally:
        if conn:
            await pool.release(conn)


async def exist_on_ftp(pool: Pool, psql_ftp_path: str, psql_modify: str, semaphore):
    # start = time.time()
    async with semaphore:
        while True:
            client = None
            try:
                async with aioftp.Client.context(host, port, login, password) as client:
                    ftp_path_exists = await client.exists(psql_ftp_path)
                    if ftp_path_exists:
                        info = await client.stat(psql_ftp_path)
                    else:
                        await set_psql_enddate(pool, psql_ftp_path)
                        break
                    if info['modify'] != psql_modify:
                        await set_psql_enddate(pool, psql_ftp_path)
                    break
            except ConnectionResetError:
                logger.exception(ConnectionResetError)
            finally:
                if client:
                    logger.info('Высвобождаем коннектор')
                    client.close()


async def exist_on_ftp2(pool: Pool, links: list, psql_ftp_path: str, psql_modify: str, semaphore):
    if all([(psql_ftp_path.split('/')[-1], psql_ftp_path, psql_modify, 0) not in links]):
        # logger.info((psql_ftp_path.split('/')[-1], psql_ftp_path, psql_modify,))
        logger.info(f'Путь {psql_ftp_path} с датой модификации {psql_modify} не найден.')
        async with semaphore:
            await set_psql_enddate(pool, psql_ftp_path)


async def main(pool: Pool):

    if not os.path.exists('Temp'):
        os.mkdir('Temp')

    semaphore = asyncio.Semaphore(semaphore_value)

    logger.info('Создаем таблицы.')
    await create_psql_tables(pool)

    logger.info('Получаем все пути из базы.')
    psql_list = await get_psql_paths(pool)

    logger.info('Получаем список путей с фтп (наполняем links).')
    links = []
    tasks = [
        asyncio.create_task(get_ftp_list(pool, links, folder, semaphore)) for folder in folders
    ]
    await asyncio.gather(*tasks)

    # logger.info(links)
    logger.info('Проверяем наличие этих путей на фтп. Если их нет, ставим дату окончания.')
    t_start = time.monotonic()

    tasks = [
        asyncio.create_task(exist_on_ftp2(pool, links, psql_ftp_path, psql_modify, semaphore))
        for psql_ftp_path, psql_modify in psql_list
    ]
    await asyncio.gather(*tasks)
    logger.info(f'Времени на проставление даты окончания ушло: {time.monotonic() - t_start}')

    logger.info(f'всего файлов для скачивания {len([link for link in links if link[-1] == 1])}')

    logger.info('Скачиваем файлы с фтп ЕИС в темп')
    limit_param = 5
    n, k = divmod(len(links), limit_param)
    new_arr = []
    for i in range(n):
        new_arr.append(links[:limit_param])
        links = links[limit_param:]
    new_arr.append(links)
    for temp in new_arr:
        tasks = [
            asyncio.create_task(get_data(pool, file, ftp_path, modify, semaphore)) for file, ftp_path, modify, flag in temp if flag == 1
        ]
        await asyncio.gather(*tasks)
    logger.info('Новые пути к файлам добавлены в базу')


# if __name__ == '__main__':
#     try:
#         asyncio.run(main(pool))
#     except (KeyboardInterrupt, SystemExit):
#         pass
