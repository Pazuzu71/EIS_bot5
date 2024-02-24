from datetime import datetime


from asyncpg.pool import Pool
from asyncpg.connection import Connection


from log.funcs import create_logger


logger = create_logger(__name__)


async def create_psql_tables(pool: Pool):
    """Функция для создания таблиц в базе данных PostgreSQL"""

    conn: Connection = await pool.acquire()
    try:
        # Создание таблицы zip, если она не существует
        await conn.execute('''
            create table if not exists zip
            (
                zip_id SERIAL PRIMARY KEY,
                ftp_path VARCHAR,
                modify VARCHAR,
                creationdate timestamp,
                enddate timestamp
            )
        ''')
        # Создание таблицы xml, если она не существует
        await conn.execute('''
            create table if not exists xml
            (
                zip_id INTEGER REFERENCES zip (zip_id) ON DELETE CASCADE,
                xml_id SERIAL PRIMARY KEY,
                eisdocno VARCHAR,
                eispublicationdate timestamp with time zone,
                xmlname VARCHAR
            )
        ''')
        logger.info('Таблицы созданы')
    except Exception as e:
        logger.exception('При создании таблиц возникла ошибка', e)
    finally:
        await pool.release(conn)


async def insert_psql_zip(pool: Pool, ftp_path: str, modify: str):
    """Добавление записи в таблицу zip"""

    conn: Connection = await pool.acquire()
    try:
        await conn.execute('''INSERT INTO zip (ftp_path, modify, creationdate) VALUES ($1, $2, $3)''',
                           ftp_path, modify, datetime.now()
                           )
    except Exception as e:
        logger.exception(f'Ошибка, путь {ftp_path} не было добавлен в таблицу zip')
        logger.exception('Ошибка при вставке в таблицу zip ', e)
    finally:
        await pool.release(conn)


async def insert_psql_xml(pool: Pool, row: dict):
    """Добавление записи в таблицу xml"""

    conn: Connection = await pool.acquire()
    try:

        # todo что будет, если нвйдутся две строчки без даты
        zip_id: str = await conn.fetchval("""SELECT * FROM zip WHERE ftp_path = $1 AND modify = $2 AND enddate IS NULL""",
                                     row['ftp_path'], row['modify'], column=0)
        await conn.execute("""INSERT INTO xml (zip_id, eisdocno, eispublicationdate, xmlname) VALUES ($1, $2, $3, $4)""",
                           zip_id, row['eisdocno'], row['eispublicationdate'], row['xmlname'])
    except Exception as e:
        logger.exception(f'Ошибка, файл {row["xmlname"]} не было добавлен в таблицу xml', e)
        logger.exception('Ошибка при вставке в таблицу zip ', e)
    finally:
        await pool.release(conn)


async def exist_in_psql_db(pool: Pool, ftp_path: str, modify: str):
    """Функия проверяет наличие фтп-пути в базе"""

    conn: Connection = await pool.acquire()
    try:
        is_exist = await conn.fetch("""SELECT * FROM zip WHERE ftp_path = $1 AND modify = $2 AND enddate IS NULL""",
                                    ftp_path, modify)
        await pool.release(conn)
        return is_exist
    except Exception as e:
        logger.exception(f'Ошибка поиска пути {ftp_path} с базе', e)
    finally:
        await pool.release(conn)


async def get_psql_paths(pool: Pool):
    """Функция получает список всех действующих фтп-путей из базы"""

    psql_list = []
    conn: Connection = await pool.acquire()
    try:
        psql_list = await conn.fetch("""SELECT ftp_path, modify FROM zip WHERE enddate IS NULL""")
        await pool.release(conn)
    except Exception as e:
        logger.exception(f'Ошибка. Действующие фтп-пути не получены ', e)
    finally:
        await pool.release(conn)
        if psql_list:
            return psql_list


async def set_psql_enddate(pool: Pool, ftp_path: str):
    """Функция проставляет дату окончания действия для неактуальных фтп-путей в базе"""

    conn: Connection = await pool.acquire()
    try:
        await conn.execute("""UPDATE zip set enddate = $1 WHERE ftp_path = $2""", datetime.now(), ftp_path)
        await conn.close()
        await pool.release(conn)
    except Exception as e:
        logger.exception(f'Ошибка, дата окончания действия {ftp_path} не проставлена ', e)
    finally:
        await pool.release(conn)


async def get_psql_data(pool: Pool, xml_id: int):
    """Функция, которая по id получает расположение архива на фтп и имя файла из этого архива"""

    conn: Connection = await pool.acquire()
    try:
        result = await conn.fetchrow(
            """
            SELECT zip.ftp_path, xml.xmlname
            FROM zip 
            INNER JOIN xml on zip.zip_id = xml.zip_id 
            WHERE xml.xml_id = $1;
            """,
            xml_id
        )
        await pool.release(conn)
        return result

    except Exception as e:
        print(f"An error occurred: {e}", Exception)
    finally:
        await pool.release(conn)


async def find_psql_document_id(pool: Pool, eisdocno: str):
    """Функция по номеру документа получает список из базы всех таких документов"""

    conn: Connection = await pool.acquire()
    try:
        result = await conn.fetch(
            """
            SELECT eispublicationdate, xml_id, xmlname
            FROM zip 
            INNER JOIN xml on zip.zip_id = xml.zip_id 
            WHERE zip.enddate IS NULL AND xml.eisdocno = $1;
            """,
            eisdocno
        )
        await pool.release(conn)
        return result
    except Exception as e:
        print(f"An error occurred: {e}", Exception)
    finally:
        await pool.release(conn)
