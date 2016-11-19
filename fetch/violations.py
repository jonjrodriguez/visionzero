import os
import re
import StringIO
import zipfile
import requests

COUNT = 0
BASE_DIR = "data/original/violations/"
BASE_URL = "http://www.nyc.gov/html/nypd/downloads/zip/traffic_data/"

def download(year, month):
    global COUNT

    filename = "%04d_%02d_sum_excel.zip" % (year, month)

    if os.path.isfile(BASE_DIR + filename):
        print "File %s was already downloaded" % filename
        return

    response = requests.get(BASE_URL + filename, stream=True)
    length = response.headers.get('content-length')

    if length is None or response.status_code == 404:
        print "File %s doesn't exist" % filename
        return

    print "Downloading and extracting %s" % filename
    COUNT += 1

    unzip_dir = "%s/%04d/%02d" % (BASE_DIR, year, month)
    z_file = zipfile.ZipFile(StringIO.StringIO(response.content))
    z_file.extractall(unzip_dir)

def cleanfiles():
    print "Removing unneeded files"

    # match files without numbers in filename
    pattern = r"[^0-9]+\.xlsx"

    count = 0

    for root, _, filenames in os.walk(BASE_DIR):
        for filename in [x for x in filenames if re.match(pattern, x) or not x.endswith(".xlsx")]:
            os.remove(os.path.join(root, filename))
            count += 1

    # Random file in data source
    os.remove(BASE_DIR + "2014/06/Book1.xlsx")
    count += 1

    print "Deleted %d files" % count

def renamefiles(year, month):
    unzip_dir = "%s/%04d/%02d" % (BASE_DIR, year, month)

    if os.path.isdir(unzip_dir):
        for filename in os.listdir(unzip_dir):
            os.rename(os.path.join(unzip_dir, filename), os.path.join(unzip_dir, "%04d--%02d--%s" % (year, month, filename)))

def main():
    if not os.path.exists(BASE_DIR):
        os.makedirs(BASE_DIR)

    for year in range(2012, 2017):
        for month in range(1, 13):
            download(year, month)

    print "Downloaded %d files" % COUNT

    cleanfiles()

    print "Renaming file"
    for year in range(2012, 2017):
        for month in range(1, 13):
            renamefiles(year, month)

if __name__ == "__main__":
    main()