from xml.dom.minidom import parse
import pandas as pd

def getMetrics():
    doc=parse("/Users/ssun206/Desktop/Workspaces/netsuite_testing_tools/resources/netsuite/xml/currency.xml")
    root=doc.documentElement
    recordList=root.getElementsByTagName('platformCore:record')
    result = []

    for record in recordList:
        nameRecord = record.getElementsByTagName("listAcct:name")[0]
        name = nameRecord.childNodes[0].data
        symbolRecord = record.getElementsByTagName("listAcct:symbol")[0]
        symbol = symbolRecord.childNodes[0].data
        isBaseCurrencyRecord = record.getElementsByTagName("listAcct:isBaseCurrency")[0]
        isBaseCurrency = isBaseCurrencyRecord.childNodes[0].data
        isInactiveRecord = record.getElementsByTagName("listAcct:isInactive")[0]
        isInactive = isInactiveRecord.childNodes[0].data
        overrideCurrencyFormatRecord = record.getElementsByTagName("listAcct:overrideCurrencyFormat")[0]
        overrideCurrencyFormat = overrideCurrencyFormatRecord.childNodes[0].data
        displaySymbolRecord = record.getElementsByTagName("listAcct:displaySymbol")[0]
        displaySymbol = displaySymbolRecord.childNodes[0].data
        symbolPlacementRecord = record.getElementsByTagName("listAcct:symbolPlacement")[0]
        symbolPlacement = symbolPlacementRecord.childNodes[0].data
        localeRecord = record.getElementsByTagName("listAcct:locale")[0]
        locale = localeRecord.childNodes[0].data
        formatSampleRecord = record.getElementsByTagName("listAcct:formatSample")[0]
        formatSample = formatSampleRecord.childNodes[0].data
        exchangeRateRecord = record.getElementsByTagName("listAcct:exchangeRate")[0]
        exchangeRate = exchangeRateRecord.childNodes[0].data
        fxRateUpdateTimezone_flag = record.getElementsByTagName("listAcct:fxRateUpdateTimezone")
        #print("fxRateUpdateTimezone_flag：%s" % fxRateUpdateTimezone_flag)
        if len(fxRateUpdateTimezone_flag):
            fxRateUpdateTimezoneRecord = record.getElementsByTagName("listAcct:fxRateUpdateTimezone")[0]
            fxRateUpdateTimezone = fxRateUpdateTimezoneRecord.childNodes[0].data
        else:
            fxRateUpdateTimezone = ''
        #print("fxRateUpdateTimezone：%s" % fxRateUpdateTimezone)
        currencyPrecisionRecord = record.getElementsByTagName("listAcct:currencyPrecision")[0]
        currencyPrecision = currencyPrecisionRecord.childNodes[0].data
        result.append(dict(name=name,
                           symbol=symbol,
                           isBaseCurrency=isBaseCurrency,
                           isInactive=isInactive,
                           overrideCurrencyFormat=overrideCurrencyFormat,
                           displaySymbol=displaySymbol,
                           symbolPlacement=symbolPlacement,
                           locale=locale,
                           formatSample=formatSample,
                           exchangeRate=exchangeRate,
                           fxRateUpdateTimezone=fxRateUpdateTimezone,
                           currencyPrecision=currencyPrecision))

    return result

df = pd.DataFrame(getMetrics(), columns=["name",
                                         "symbol",
                                         "isBaseCurrency",
                                         "isInactive",
                                         "overrideCurrencyFormat",
                                         "displaySymbol",
                                         "symbolPlacement",
                                         "locale",
                                         "formatSample",
                                         "exchangeRate",
                                         "fxRateUpdateTimezone",
                                         "currencyPrecision"])          #Form Dataframe
print(df)

df.to_csv("/Users/ssun206/Desktop/Workspaces/netsuite_testing_tools/resources/netsuite/csv/currency.csv")     #Write to CSV.