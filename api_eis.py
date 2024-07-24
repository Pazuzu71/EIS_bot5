import asyncio
import uuid
import re
import os


import aiohttp
import aiofiles


from app4_config import *


# regNums = ['03662000256', '03662000020']


async def get_response(subsystemType, exactDate, regNum):

    endpoint = "https://int44.zakupki.gov.ru/eis-integration/services/getDocsMis2"

    data = f"""
    <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ws="http://zakupki.gov.ru/fz44/get-docs-mis/ws">
       <soapenv:Header/>
       <soapenv:Body>
          <ws:getPublicDocsRequest>
             <index>
                <id>{uuid.uuid4()}</id>
                <sender>agent_007</sender>
                <createDateTime>2024-07-01T10:38:07.553</createDateTime>
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

    async with aiohttp.ClientSession() as client:
        response = await client.post(url=endpoint, data=data, ssl=False)
        response = await response.text()
        archiveUrl = re.search(r'(?<=<archiveUrl>).+(?=</archiveUrl>)', response)
        if archiveUrl:
            archiveUrl = archiveUrl[0]
            return regNum, archiveUrl
        return regNum, None


async def get_file(regNum, archiveUrl, semaphore):
    async with semaphore:
        async with aiohttp.ClientSession() as client:
            async with client.get(url=archiveUrl, ssl=False) as response:
                async with aiofiles.open(f'downloaded//{regNum}.zip', mode='wb') as f:
                    src = await response.read()
                    await f.write(src)


async def main():

    try:
        os.mkdir('downloaded')
    except FileExistsError:
        pass

    with open('kspz.txt', encoding='utf-8') as f:
        regNums = [kspz.strip() for kspz in f.readlines()]

    semaphore = asyncio.Semaphore(5)

    tasks = [asyncio.create_task(get_response(subsystemType, exactDate, regNum)) for regNum in regNums]
    archiveUrls = await asyncio.gather(*tasks)
    archiveUrls = [archiveUrl for archiveUrl in archiveUrls if archiveUrl[-1]]
    print(archiveUrls)

    tasks = [asyncio.create_task(get_file(regNum, archiveUrl, semaphore)) for regNum, archiveUrl in archiveUrls]
    await asyncio.gather(*tasks)


if __name__ == '__main__':
    asyncio.run(main())
