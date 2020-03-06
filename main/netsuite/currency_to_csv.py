from xml.dom.minidom import parse
import csv

from main.path import get_xml_path

fileName_model = '/Users/ssun206/Desktop/Workspaces/netsuite_testing_tools/resources/netsuite/model/currency_model.csv'
fileName_xml = '/Users/ssun206/Desktop/Workspaces/netsuite_testing_tools/resources/netsuite/xml/currency_0_0.xml'
fileName_csv = '/Users/ssun206/Desktop/Workspaces/netsuite_testing_tools/resources/netsuite/csv/currency_new.csv'


def getRow():
    with open(fileName_model, 'rt') as csvfile:
        reader = csv.reader(csvfile)
        column = [row[0] for row in reader]
    # print(column)
    return column


def get_sync_info(xml_string):
    doc = parse(xml_string)
    root = doc.documentElement
    search_id = root.getElementsByTagName('platformCore:searchId')
    total_pages = root.getElementsByTagName('platformCore:totalPages')
    return search_id, total_pages


def getMetrics():
    doc = parse(fileName_xml)
    root = doc.documentElement
    recordList = root.getElementsByTagName('platformCore:record')
    columnList = getRow()
    rows = []
    for record in recordList:
        row = 0
        row_1 = []
        while row < len(columnList):
            name = columnList[row]
            record_flag = record.getElementsByTagName("listAcct:" + name)
            if len(record_flag):
                nameRecord = record.getElementsByTagName("listAcct:" + name)[0]
                name = nameRecord.childNodes[0].data
            else:
                name = ''
            row += 1
            row_1.append(name)
        # print("row_1：%s" % row_1)
        rows.append(row_1)
    # print("rows：%s" % rows)
    return rows

def write_xml(fileName_csv):
    # Write to CSV.
    with open(fileName_csv, 'w+') as csvfile:
        # Create a csv writer object
        csvwriter = csv.writer(csvfile)
        # Writing the column
        csvwriter.writerow(getRow())
        # Writing the data rows
        csvwriter.writerows(getMetrics())


if __name__ == '__main__':
    search_id, total_pages = get_sync_info(get_xml_path()+'/customer_0_0.xml')
    print(search_id, total_pages)

