import asyncio
import uuid
import re
import os
import time
import zipfile


import aiohttp
from aiohttp.client_exceptions import ClientConnectorError, ServerDisconnectedError, ClientOSError
import aiofiles


from app4_config import *


# regNums = ['03662000256', '03662000020']


async def get_response(subsystemType, exactDate, regNum, semaphore):

    endpoint = "https://int44.zakupki.gov.ru/eis-integration/services/getDocsMis2"

    data = f"""
    <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ws="http://zakupki.gov.ru/fz44/get-docs-mis/ws">
       <soapenv:Header/>
       <soapenv:Body>
          <ws:getPublicDocsRequest>
             <index>
                <id>{uuid.uuid4()}</id>
                <sender>agent_007</sender>
                <createDateTime>2024-07-30T10:38:07.553</createDateTime>
                <mode>PROD</mode>
             </index>
             <!--You have a CHOICE of the next 2 items at this level-->
             <selectionParams44>
                <subsystemType>{subsystemType}</subsystemType>
                 <periodInfo>
                   <exactDate>{exactDate}</exactDate>
                </periodInfo>
                <organizations44Info>
                   <!--1 to 1000 repetitions:-->
                   <organization44Info>
                      <!--You have a CHOICE of the next 2 items at this level-->
                      <regNum>{regNum}</regNum>
                   </organization44Info>
                </organizations44Info>
             </selectionParams44>
          </ws:getPublicDocsRequest>
       </soapenv:Body>
    </soapenv:Envelope>
    """
    async with semaphore:

        async with aiohttp.ClientSession() as client:
            while True:
                try:
                    response = await client.post(url=endpoint, data=data, ssl=False)
                    break
                except (ClientConnectorError, ServerDisconnectedError, ClientOSError) as e:
                    print(e)

            response = await response.text()
            response = response.replace('</archiveUrl>', '</archiveUrl>\n')
            archiveUrls = re.findall(r'(?<=<archiveUrl>).+(?=</archiveUrl>)', response)

        if archiveUrls:
            # archiveUrl = archiveUrl[0]
            return regNum, archiveUrls
        else:
            print(f'No archiveUrl for {regNum}')
            with open('error.txt', 'a', encoding='utf-8') as out:
                out.write(response)
            return regNum, None


async def get_file(regNum, archiveUrl, semaphore):
    while True:
        try:
            async with semaphore:
                async with aiohttp.ClientSession() as client:
                    async with client.get(url=archiveUrl, ssl=False) as response:
                        async with aiofiles.open(f'downloaded//{exactDate}//{subsystemType}//{regNum}.zip', mode='wb') as f:
                            src = await response.read()
                            time.sleep(0.1)
                            await f.write(src)
                        time.sleep(1)
            break
        except Exception as e:
            print('error while loading zip', e)
    try:
        with zipfile.ZipFile(f'downloaded//{exactDate}//{subsystemType}//{regNum}.zip') as z:
            for file in z.namelist():
                if file.endswith('.xml'):
                    z.extract(file, f'downloaded//{exactDate}//{subsystemType}')
    except zipfile.BadZipFile as e:
        print(e, regNum)
    # os.unlink(f'downloaded//{exactDate}//{subsystemType}//{regNum}.zip')
        # z.extractall(f'downloaded//{exactDate}//{subsystemType}')


async def main():

    try:
        os.mkdir('downloaded')
    except FileExistsError:
        pass

    try:
        os.mkdir(f'downloaded//{exactDate}')
    except FileExistsError:
        pass

    try:
        os.mkdir(f'downloaded//{exactDate}//{subsystemType}')
    except FileExistsError:
        pass

    for path, dirs, files in os.walk(f'downloaded//{exactDate}//{subsystemType}'):
        if files:
            for file in files:
                os.unlink(os.path.join(path, file))
        print('Files deleted')

    with open('error.txt', 'w') as out:
        out.write('')

    with open('kspz.txt', encoding='utf-8') as f:
        regNums = [kspz.strip() for kspz in f.readlines()]

    semaphore = asyncio.Semaphore(5)

    for subsystemType in subsystemTypes.split(','):
        tasks = [asyncio.create_task(get_response(subsystemType, exactDate, regNum, semaphore)) for regNum in regNums]
        archiveUrls_all = await asyncio.gather(*tasks)
        urls = []
        for archiveUrls in archiveUrls_all:
            if archiveUrls[-1] and len(archiveUrls[-1]) > 1:
                print(archiveUrls[0], 'Несколько архивов')
                i = 0
                for url in archiveUrls[-1]:
                    urls.append((f'{archiveUrls[0]}_{i}', url))
                    i += 1
            elif archiveUrls[-1] and len(archiveUrls[-1]) == 1:
                urls.append((archiveUrls[0], archiveUrls[-1][0]))

        # archiveUrls_all = [archiveUrl for archiveUrl in archiveUrls_all if archiveUrl[-1]]
        # print(archiveUrls_all)
        # print(urls)

        tasks = [asyncio.create_task(get_file(regNum_, archiveUrl, semaphore)) for regNum_, archiveUrl in urls]
        await asyncio.gather(*tasks)


if __name__ == '__main__':
    asyncio.run(main())
