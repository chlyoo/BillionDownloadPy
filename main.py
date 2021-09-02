from sync_download import download_files

if __name__ =="__main__":
    url = 'https://aws-tc-largeobjects.s3.dualstack.us-west-2.amazonaws.com?list-type=2'
    download_files(url, 'output/sync')