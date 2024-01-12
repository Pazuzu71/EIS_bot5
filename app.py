import asyncio
import aioftp


async def get_ftp_list(host, port, login, password):
    async with aioftp.Client.context(host, port, login, password) as client:
        x = await client.list('/fcs_regions/Tulskaja_obl/contracts/currMonth', recursive=False)
    for path, info in x:
        res.append(path)


async def main():
    tasks = [
        asyncio.create_task(get_ftp_list(host, port, login, password))
    ]
    await asyncio.wait(tasks)


if __name__ == '__main__':
    host, port, login, password = 'ftp.zakupki.gov.ru', 21, 'free', 'free'
    res = []
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print('Ошибка, останов бота!')
    for p in res:
        print(p)