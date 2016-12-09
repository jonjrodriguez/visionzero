import csv
import StringIO

def save_csv(data):
    si = StringIO.StringIO()

    writer = csv.writer(si)
    for row in data:
        writer.writerow(row)

    return si.getvalue()
