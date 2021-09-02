from pathlib import Path
import requests
import os
from bs4 import BeautifulSoup

def download_files(url, outdir):
    r = requests.get(url)
    soup = BeautifulSoup(r.content, features='xml')
    while True:
        content = soup.findAll('Contents')
        last_content = content[-1]
        content = content[:-2]
        for _, item in enumerate(content):
            filename = item.find('Key').text
            if filename.split('/')[-1] == "":
                continue
            remote_url = f'https://aws-tc-largeobjects.s3.dualstack.us-west-2.amazonaws.com/{filename}'
            download_file(remote_url,
                          os.path.join(outdir, filename))
        if soup.IsTruncated is not None:
            if soup.IsTruncated.text == 'true':
                next_page = url +"&nextToken="+ soup.NextContinuationToken.text + "&start-after=" + last_content.find('Key').text
                r = requests.get(next_page)
                soup = BeautifulSoup(r.content, features='xml')
            else:
                break


def download_file(remote_url, local_filename):
    response = requests.get(remote_url)
    response.raise_for_status()
    output_file = Path(local_filename)
    output_file.parent.mkdir(exist_ok=True, parents=True)
    output_file.write_bytes(response.content)

