import os
import sys
import requests

COUNT = 0
BASE_DIR = "data/original/tlc/"
BASE_URL = "https://s3.amazonaws.com/nyc-tlc/trip+data/"

def download(color, year, month):
    global COUNT

    filename = "%s_tripdata_%04d-%02d.csv" % (color, year, month)

    if os.path.isfile(BASE_DIR + filename):
        print "File %s was already downloaded" % filename
        return

    response = requests.get(BASE_URL + filename, stream=True)
    length = response.headers.get('content-length')

    if length is None:
        print "File %s doesn't exist" % filename
        return

    with open(BASE_DIR + filename, "wb") as output:
        print "Downloading %s" % filename
        COUNT += 1
        downloaded = 0

        for chunk in response.iter_content(chunk_size=4096):
            if chunk:
                downloaded += len(chunk)
                output.write(chunk)
                done = 50 * downloaded / int(length)
                sys.stdout.write("\r[%s%s]" % ('=' * done, ' ' * (50-done)))
                sys.stdout.flush()

    print "\n"

def main():
    if not os.path.exists(BASE_DIR):
        os.makedirs(BASE_DIR)

    for year in range(2012, 2017):
        for month in range(1, 13):
            download("yellow", year, month)

    for year in range(2013, 2017):
        for month in range(1, 13):
            download("green", year, month)

    print "Downloaded %d files" % COUNT

if __name__ == "__main__":
    main()
