import os
import requests
import aiohttp
import aiofiles
from bs4 import BeautifulSoup
from pathlib import Path
from concurrent import futures



class Downloader:
    def __init__(self, host, outdir):
        self.host = host
        self.outdir = outdir
        self.session = requests.session()

    def download(self, filename):
        remote_url = f'https://aws-tc-largeobjects.s3.dualstack.us-west-2.amazonaws.com/{filename}'
        print(remote_url)
        response = self.session.get(remote_url)
        response.raise_for_status()
        output_file = Path(os.path.join(self.outdir, filename))
        output_file.parent.mkdir(exist_ok=True, parents=True)
        output_file.write_bytes(response.content)

    async def async_download(self, filename):
        self.session = aiohttp.ClientSession()
        remote_url = f'https://aws-tc-largeobjects.s3.dualstack.us-west-2.amazonaws.com/{filename}'
        async with self.session.get(remote_url) as resp:
            if resp.status == 200:
                file_path = os.path.join(self.outdir, filename)
                output_file = Path(file_path)
                output_file.parent.mkdir(exist_ok=True, parents=True)
                f = await aiofiles.open(file_path, mode='wb')
                await f.write(await resp.read())
                await f.close()

def iter_all_pages(soup):
    # pages = []
    while True:
        content = soup.findAll('Contents')
        last_content = content[-1]
        last_key = last_content.find('Key').text
        content = content[:-1]
        page = [key for key in map(lambda x: x.find('Key').text, content) if key.split('/')[-1] != "" ]
        yield page
        if soup.IsTruncated is not None:
            if soup.IsTruncated.text == 'true':
                next_page = url + "&start-after=" + last_key
                r = requests.get(next_page)
                soup = BeautifulSoup(r.content, features='xml')
            else:
                return #pages

def download_files(url, outdir, start_after=None):
    r = requests.get(url)
    if start_after is not None:
        r = requests.get(url + "&start-after="+start_after)
    soup = BeautifulSoup(r.content, features='xml')
    all_pages = iter_all_pages(soup)
    downloader = Downloader(url, outdir)
    with futures.ProcessPoolExecutor() as executor:
        for page in all_pages:
            future_to_filename = {}
            for filename in page:
                future = executor.submit(downloader.async_download, filename)
                future_to_filename[future] = filename
            for future in futures.as_completed(future_to_filename):
                # future.result()
                await asyncio.gather(*future)

if __name__ =="__main__":
    url = 'https://aws-tc-largeobjects.s3.dualstack.us-west-2.amazonaws.com?list-type=2'
    start_after =  "AWS-200-BIG/v3.1/lab-6-spark/trip data/yellow_tripdata_2014-05.csv"
    # download_files(url, 'output/multiprocess/')
    download_files(url, 'output/multiprocess/', start_after)