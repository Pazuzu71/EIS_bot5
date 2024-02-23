import asyncio
import os
import re
import zipfile
import time
from datetime import datetime


import aioftp
from asyncpg.pool import Pool


from log.funcs import create_logger
from config import semaphore_value, host, port, login, password, folders, limit_param
from sql.funcs import create_psql_tables, insert_psql_zip, insert_psql_xml, get_psql_paths, set_psql_enddate
from sql.funcs import exist_in_psql_db


logger = create_logger(__name__)


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
                if any(sub_path in str(path) for sub_path in ['currMonth', 'prevMonth'] + ['2022', '2023', '2024']):
                    links.append(
                        (str(path).split('/')[-1], str(path), info['modify'], 1)
                    )
            elif info['size'] != '22' and str(path).endswith('.zip'):
                # TODO текущий и прошлый месяцы будут всегда, года вынести в отдельный параметр
                if any(sub_path in str(path) for sub_path in ['currMonth', 'prevMonth'] + ['2022', '2023', '2024']):
                    links.append(
                        (str(path).split('/')[-1], str(path), info['modify'], 0)
                    )


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

    logger.info('Проверяем наличие этих путей на фтп. Если их нет, ставим дату окончания.')
    t_start = time.monotonic()
    tasks = [
        asyncio.create_task(exist_on_ftp2(pool, links, psql_ftp_path, psql_modify, semaphore))
        for psql_ftp_path, psql_modify in psql_list
    ]
    await asyncio.gather(*tasks)
    logger.info(f'Время на проставление даты окончания : {time.monotonic() - t_start}')

    logger.info(f'Всего файлов для скачивания {len([link for link in links if link[-1] == 1])}')
    logger.info('Скачиваем файлы с фтп ЕИС в темп')
    t_start = time.monotonic()
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
    logger.info(f'Новые пути к файлам добавлены в базу за {time.monotonic() - t_start} секунд')


# if __name__ == '__main__':
#     try:
#         asyncio.run(main(pool))
#     except (KeyboardInterrupt, SystemExit):
#         pass
