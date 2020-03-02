from xml.dom.minidom import parse
import csv


fileName_model = '/Users/ssun206/Desktop/Workspaces/netsuite_testing_tools/resources/netsuite/model/currency_model.csv'
fileName_xml = '/Users/ssun206/Desktop/Workspaces/netsuite_testing_tools/resources/netsuite/xml/currency.xml'
fileName_csv = '/Users/ssun206/Desktop/Workspaces/netsuite_testing_tools/resources/netsuite/csv/currency_new.csv'

def getRow():
    with open(fileName_model, 'rt') as csvfile:
        reader = csv.reader(csvfile)
        column = [row[0] for row in reader]
    # print(column)
    return column

def getMetrics():
    doc=parse(fileName_xml)
    root=doc.documentElement
    recordList=root.getElementsByTagName('platformCore:record')
    columnList = getRow()
    rows = []
    for record in recordList:
        row = 0
        row_1 = []
        while row < len(columnList):
            name = columnList[row]
            record_flag = record.getElementsByTagName("listAcct:"+name)
            if len(record_flag):
                nameRecord = record.getElementsByTagName("listAcct:"+name)[0]
                name = nameRecord.childNodes[0].data
            else:
                name = ''
            row += 1
            row_1.append(name)
        # print("row_1：%s" % row_1)
        rows.append(row_1)
    # print("rows：%s" % rows)
    return rows

#Write to CSV.
with open(fileName_csv, 'w+') as csvfile:
    # Create a csv writer object
    csvwriter = csv.writer(csvfile)
    # Writing the column
    csvwriter.writerow(getRow())
    # Writing the data rows
    csvwriter.writerows(getMetrics())
