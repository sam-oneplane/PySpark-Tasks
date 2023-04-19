import json
import asyncio
import aiohttp

async def get_cve(semaphore, cve, cve_url, api_key):
    url = f'{cve_url}?cveid={cve}'
    headers = {'API-KEY': api_key}

    # acquire the semaphore before making the request (released automatically after the request is complete)
    async with semaphore:
        # session for making HTTP requests
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                return await response.json()
            

async def get_cves(cves, max_concurrency, cve_url, api_key):
    # create a new asyncio.Semaphore object with a maximum concurrency value
    semaphore = asyncio.Semaphore(max_concurrency)
    # create a list of background calls to get_cve 
    tasks = [asyncio.ensure_future(get_cve(semaphore, cve, cve_url, api_key)) for cve in cves]
    # group multiple awaitables together
    results = await asyncio.gather(*tasks)
    return results


async def get(cves, max_concurrency, cve_url, api_key):
    cves_list = await get_cves(cves, max_concurrency, cve_url, api_key)
    for cve, info in zip(cves, cves_list):
        with open(f'{cve}.json', 'w') as f:
            json.dump(info, f, indent=4)



if __name__ == "__main__":
    url = 'https://services.nvd.nist.gov/rest/json/cves/2.0'
    api_key = '0ff17eff-73b8-4a3f-b6e0-8e71d5e0091a' 
    cves = ['CVE-1999-0095', 'CVE-1999-0082', 'CVE-1999-1471']
    max_concurrency = 10
    # run asynchronous tasks 
    loop = asyncio.get_event_loop()
    loop.run_until_complete(get(cves, max_concurrency, url, api_key))