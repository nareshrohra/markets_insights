import pandas as pd, numpy as np, seaborn as sns
from matplotlib import pyplot as plt
import os
from datetime import date, timedelta, datetime
from IPython.core.display import display
from urllib.error import HTTPError

import plotly.graph_objects as go
import plotly.express as px

from urllib.request import urlopen
import urllib.error
from zipfile import ZipFile

class DataSet:
    bhavcopyOld: None
    bhavcopyNew: None
    opOld: None
    opNew: None

    def __init__(self, bcOld, bcNew, oOld, oNew):
        self.bhavcopyOld = bcOld
        self.bhavcopyNew = bcNew
        self.opOld = oOld
        self.opNew = oNew

    def printSampleData(self):
        print(self.bhavcopyOld.head(1))
        print(self.bhavcopyNew.head(1))
        print(self.opOld.head(1))
        print(self.opNew.head(1))

class DataReader:
    def readFundamentals(self, path):
        return pd.read_csv(path)

    def getMarketHolidaysList(self):
        return marketHolidays
    
    def isDataAvailableForDate(self, current_date):
        day = current_date.strftime("%A")
        return not (day in weekends or current_date in self.getMarketHolidaysList() or 
            current_date in self.getDaysToSkipForIssues())
            
    def getRecentDates(self, days_count = 3):
        today = datetime.today()
        recent_days = []

        if today.hour > 20:
            recent_days.append(today.date())

        current_date = today
        while len(recent_days) < days_count:
            current_date = current_date - timedelta(days=1)
            if self.isDataAvailableForDate(current_date):
                recent_days.append(current_date)

        return recent_days
        
    def getWeekends(self):
        return weekends
    
    def getDaysToSkipForIssues(self):
        return daysToSkipForIssues
    
    def downloadParticipantWiseOiData(self, date):
        #https://www1.nseindia.com/content/nsccl/fao_participant_oi_29092020.csv
        basePath = "https://www1.nseindia.com/content/nsccl/"
        fileName = "fao_participant_oi_" + date.strftime("%d%m%Y").upper() + ".csv"
        url = basePath + fileName
        print(url)
        urldata = urlopen(url)
        with open('data/' + fileName,'wb') as output:
            output.write(urldata.read())

    def readParticipantWiseOiData(self, date):
        fileName = "fao_participant_oi_" + date.strftime("%d%m%Y").upper() + ".csv"
        filepath = "Data/" + fileName
        
        if not os.path.isfile(filepath):
            self.downloadParticipantWiseOiData(date)
        
        df = pd.read_csv(filepath, skiprows=1)
        df['ForDate'] = date
        df.columns = df.columns.str.strip()
        return df
    
    def readParticipantWiseOiDataForDateRange(self, fromDate, toDate = date.today()):
        datelist = pd.date_range(fromDate, toDate, freq='B').tolist()
        combined_df = None
        for date in datelist:
            if date not in marketHolidays:
                if combined_df is None:
                    combined_df = self.readParticipantWiseOiData(date)
                else:
                    try:
                        df = self.readParticipantWiseOiData(date)
                        combined_df = combined_df.append(df, ignore_index=True)
                    except urllib.error.HTTPError as e:
                        print(e, date)
        return combined_df

    def downloadExtractZip(self, zipurl, fileName, extractPath):
        zipresp = urlopen(zipurl)
        zipPath = "Data/" + fileName
        tempzip = open(zipPath, "wb")
        tempzip.write(zipresp.read())
        tempzip.close()
        zf = ZipFile(zipPath)
        zf.extractall(path = "Data/" + extractPath)
        zf.close()

    def trimDatapointValues(self, data, datapoints):
        for dp in datapoints:
            data[dp] = data[dp].apply(lambda x: str(x).strip())
    
    def readFile(self, filepath):
        df = pd.read_csv(filepath)
        df.columns = df.columns.str.strip()
        return df

    def downloadBhavCopy(self, date):
        #https://www1.nseindia.com/content/historical/EQUITIES/2020/JUN/cm12JUN2020bhav.csv.zip
        basePath = "https://www1.nseindia.com/content/historical/EQUITIES/" + date.strftime("%Y") + "/" + date.strftime("%b").upper() + "/"
        fileName = "cm" + date.strftime("%d%b%Y").upper() + "bhav.csv" + ".zip"
        url = basePath + fileName
        print(url)
        self.downloadExtractZip(url, fileName, "")

    def downloadBhavCopy2(self, date):
        #https://www1.nseindia.com/archives/equities/bhavcopy/pr/PR080121.zip
        basePath = "https://www1.nseindia.com/archives/equities/bhavcopy/pr/"
        folderName = "PR" + date.strftime("%d%m%y").upper()
        fileName = folderName + ".zip"
        url = basePath + fileName
        print(url)
        self.downloadExtractZip(url, fileName, folderName)
        
    def readBhavcopy2(self, date):
        fileName = "Pd" + date.strftime("%d%m%y").upper() + ".csv"
        filepath = "Data/PR" + date.strftime("%d%m%y").upper() + "/" + fileName
        
        if not os.path.isfile(filepath):
            self.downloadBhavCopy2(date)
        
        df = pd.read_csv(filepath)
        df.columns = df.columns.str.strip()
        df.rename(columns = {
            'OPEN_PRICE': 'OPEN',
            'HIGH_PRICE': 'HIGH',
            'LOW_PRICE': 'LOW',
            'CLOSE_PRICE': 'CMP'
          }, inplace=True)
        self.trimDatapointValues(df, ['SYMBOL', 'SECURITY'])
        df['ForDate'] = date
        df = df[(df['SERIES'] == 'EQ') | (df['SECURITY'].isin(['Nifty 50', 'Nifty Bank']))]
        df = df[df['MKT'].isin(['Y', 'N', 'G'])]
        df = df.astype({'PREV_CL_PR': 'float', 
                              'OPEN': 'float', 
                              'HIGH': 'float', 
                              'LOW': 'float', 
                              'CMP': 'float', 
                              'NET_TRDVAL': 'float', 
                              'NET_TRDQTY': 'float',
                              'TRADES': 'int64',
                              'HI_52_WK': 'float', 
                              'LO_52_WK': 'float'})
        df['SYMBOL'] = df.apply(lambda x: x['SYMBOL'] if x['MKT'] == 'N' else x['SECURITY'], axis=1)
        df.loc[df['SYMBOL'] == 'Nifty 50', 'SYMBOL'] = 'NIFTY'
        df.loc[df['SYMBOL'] == 'Nifty Bank', 'SYMBOL'] = 'BANKNIFTY'
        #df['OPEN'] = pd.to_numeric(df['OPEN'], downcast='float')
        return df

    def readBhavcopy(self, date, niftyBhav=None):
        fileName = "cm" + date.strftime("%d%b%Y").upper() + "bhav.csv"
        filepath = "Data/" + fileName
        
        if not os.path.isfile(filepath):
            self.downloadBhavCopy(date)
        
        df = pd.read_csv(filepath)
        df.columns = df.columns.str.strip()
        self.trimDatapointValues(df, ['SYMBOL'])
        df['ForDate'] = date
        df = df[df['SERIES'] == 'EQ']
        if (niftyBhav != None):
            df = df.append({'SYMBOL':'NIFTY', 'CLOSE': niftyBhav}, ignore_index=True)
        return df

    def readBhavCopyForDates(self, dates):
        bhavCopies = []
        for day in dates:
            bhavCopies.append(self.readBhavcopy2(day))
        return bhavCopies

    def readBhavcopyForDateRange(self, fromDate, toDate = date.today()):
        datelist = pd.date_range(fromDate, toDate, freq='B').tolist()
        combined_df = None
        for date in datelist:
            if self.isDataAvailableForDate(date):
                if combined_df is None:
                    combined_df = self.readBhavcopy2(date)
                else:
                    try:
                        df = self.readBhavcopy2(date)
                        combined_df = combined_df.append(df, ignore_index=True)
                    except urllib.error.HTTPError as e:
                        print(e, date)
        return combined_df

    def downloadFOData(self, date):
        #https://www1.nseindia.com/archives/fo/mkt/fo12062020.zip
        basePath = "https://www1.nseindia.com/archives/fo/mkt/"
        folderName = "fo" + date.strftime("%d%m%Y")
        fileName = folderName + ".zip"
        url = basePath + fileName
        print(url)
        self.downloadExtractZip(url, fileName, folderName)

    def readFuturesData(self, date):
        folderName = "fo" + date.strftime("%d%m%Y")
        path = "Data/" + folderName
        
        if not os.path.isdir(path):
            self.downloadFOData(date)
        
        df = pd.read_csv(path + "/" + "fo" + date.strftime("%d%m%Y") + ".csv")
        df.columns = df.columns.str.strip()
        self.trimDatapointValues(df, ['SYMBOL'])
        df['ForDate'] = date.strftime("%Y-%m-%d")
        return df

    def readOptionsDataForDateRange(self, fromDate, toDate = date.today()):
        datelist = pd.date_range(fromDate, toDate, freq='B').tolist()
        combined_df = None
        for date in datelist:
            if date not in marketHolidays:
                if combined_df is None:
                    combined_df = self.readOptionsData(date)
                else:
                    try:
                        df = self.readOptionsData(date)
                        combined_df = combined_df.append(df, ignore_index=True)
                    except urllib.error.HTTPError as e:
                        print(e, date)
        return combined_df

    def readOptionsData(self, date):
        folderName = "fo" + date.strftime("%d%m%Y")
        path = "Data/" + folderName
        
        if not os.path.isdir(path):
            self.downloadFOData(date)

        filepath = path + "/" + "op" + date.strftime("%d%m%Y") + ".csv"
        df = self.readFile(filepath)
        df['ForDate'] = date.strftime("%Y-%m-%d")
        self.trimDatapointValues(df, ['SYMBOL', 'OPT_TYPE'])
        return df

    def downloadEquityDeliveryData(self, date):
        #https://www1.nseindia.com/archives/equities/mto/MTO_23112020.DAT
        basePath = "https://www1.nseindia.com/archives/equities/mto/"
        fileName = "MTO_" + date.strftime("%d%m%Y") + ".DAT"
        url = basePath + fileName
        print(url)
        urldata = urlopen(url)
        with open('data/' + fileName, 'wb') as output:
            output.write(urldata.read())
    
    def readEquityDeliveryData(self, date):
        fileName = "MTO_" + date.strftime("%d%m%Y") + ".DAT"
        filepath = "Data/" + fileName
        
        if not os.path.isfile(filepath):
            self.downloadEquityDeliveryData(date)
        
        df = pd.read_csv(filepath, skiprows=4, names=['RecordTypeCode', 'SrNo', 'SYMBOL', 'RecordType', 'QuantityTraded', 'DeliverableQuantity', 'DeliverableQuantityPercent'])
        df['ForDate'] = date
        df.columns = df.columns.str.strip()
        return df

    def readEquityDeliveryDataForDateRange(self, fromDate, toDate = date.today()):
        datelist = pd.date_range(fromDate, toDate, freq='B').tolist()
        combined_df = None
        for date in datelist:
            if date not in marketHolidays:
                if combined_df is None:
                    combined_df = self.readEquityDeliveryData(date)
                else:
                    try:
                        df = self.readEquityDeliveryData(date)
                        combined_df = combined_df.append(df, ignore_index=True)
                    except urllib.error.HTTPError as e:
                        print(e, date)
        return combined_df

class CompiledData:
    old: None
    new: None
    keyColumns: None

    def __init__(self, old, new):
        self.old = old
        self.new = new
        self.keyColumns = ['SYMBOL', 'OPT_TYPE','STR_PRICE', 'EXP_DATE', 'CMP', 'Moneyness', \
         'StrikePriceDiff', 'StrikePriceDiffPer', 'StrikePriceDiffPerAbs', 'LotSize', 'Premium', 'SpotAdjPremium', 'SpotPriceGrowthPer', 'PriceGrowthPer', 'PremiumGrowth', 'PremiumGrowthPer']

class DataProcessor:
    originalData: None
    compiledData: None
    indexCols: None
    optionsDisplayColumns: None
    futDisplayCols: None
    reader: None
    niftySymbols: None
    bankniftySymbols: None

    def __init__(self):
        self.reader = DataReader()
        self.optionsDisplayColumns = ['SYMBOL', 'STR_PRICE', 'OPT_TYPE', 'CMP', 'CLOSE_PRICE', 'Premium', 'OPEN_INT*', 'LotSize', 'StrikePriceDiffPer', 'OpIntradayChange', 'CmpIntradayChange']
        self.futDisplayCols = ['SYMBOL', 'EXP_DATE', 'CLOSE_PRICE', 'CMP', 'Fwd', 'OPEN_INT*', 'PriceDiffPer', 'PriceDiffPerAbs', 'FutureIntradayChange', 'CmpIntradayChange']
        self.niftySymbols = ['ADANIPORTS', 'ASIANPAINT', 'AXISBANK', 'BAJAJ-AUTO', 'BAJAJFINSV', 'BAJFINANCE', 'BHARTIARTL', 'BPCL', 'BRITANNIA', 'CIPLA', 'COALINDIA', 'DIVISLAB', 'DRREDDY', 'EICHERMOT', 'GAIL', 'GRASIM', 'HCLTECH', 'HDFC', 'HDFCBANK', 'HDFCLIFE', 'HEROMOTOCO', 'HINDALCO', 'HINDUNILVR', 'ICICIBANK', 'INDUSINDBK', 'INFY', 'IOC', 'ITC', 'JSWSTEEL', 'KOTAKBANK', 'LT', 'M&M', 'MARUTI', 'NESTLEIND', 'NTPC', 'ONGC', 'POWERGRID', 'RELIANCE', 'SBILIFE', 'SBIN', 'SHREECEM', 'SUNPHARMA', 'TATAMOTORS', 'TATASTEEL', 'TCS', 'TECHM', 'TITAN', 'ULTRACEMCO', 'UPL', 'WIPRO']
        self.bankniftySymbols = ['RBLBANK', 'AXISBANK', 'HDFCBANK', 'SBIN', 'ICICIBANK', 'BANDHANBNK', 'KOTAKBANK', 'PNB', 'FEDERALBNK', 'INDUSINDBK', 'BANKBARODA', 'IDFCFIRSTB']
    
    # ## Read and standardise data
    def readData(self, dateOld, dateNew):
        bhavcopyOld = self.reader.readBhavcopy(dateOld)
        bhavcopyNew = self.reader.readBhavcopy(dateNew)
        optionsOld = self.reader.readOptionsData(dateOld)
        optionsNew = self.reader.readOptionsData(dateNew)

        self.originalData = DataSet(bhavcopyOld, bhavcopyNew, optionsOld, optionsNew)

    def mergeBhavAndOp(self, op, bhav):
        mergedData = pd.merge(op, bhav, how='inner', left_on=['SYMBOL'], right_on=['SYMBOL'])
        mergedData.rename(columns={'CLOSE': 'CMP'}, inplace=True)
        return mergedData

    def mergeFundamentalsAndOp(self, op, fdm):
        fdmDisplayCols = ['Ticker', 'Last Close', 'Price 52 Wk Low', 'Price 52 Wk High', '% above 52w low']
        mergedData = pd.merge(op, fdm[fdmDisplayCols], how='inner', left_on=['SYMBOL'], right_on=['Ticker'])
        return mergedData

    def mergeBhavAndFutures(self, futures, bhav):
        mergedData = pd.merge(futures, bhav, how='inner', left_on=['SYMBOL'], right_on=['SYMBOL'])
        mergedData.rename(columns={'CLOSE': 'CMP'}, inplace=True)
        return mergedData

    def compile(self, niftyOldBhav, niftyNewBhav):
        newData = self.compileData(self.originalData.opNew, self.originalData.bhavcopyNew, None, niftyNewBhav)
        oldData = self.compileData(self.originalData.opOld, self.originalData.bhavcopyOld, newData, niftyOldBhav)
        self.compiledData = CompiledData(oldData, newData)
        return self.compiledData

    def trimDatapointValues(self, data, datapoints):
        for dp in datapoints:
            data[dp] = data[dp].apply(lambda x: str(x).strip())

    def compileData(self, op, bhavcopy, newCompiledData, niftyBhav):
        self.trimDatapointValues(op, ['SYMBOL', 'OPT_TYPE'])
        compiledData = self.mergeBhavAndOp(op, bhavcopy)
        self.computeContractData(compiledData)
        
        if newCompiledData is not None:
            self.computeOldAndNewVariances(newCompiledData, compiledData)
        
        return compiledData

    def computeContractData(self, mergedData):
        mergedData['Moneyness'], mergedData['StrikePriceDiff'], mergedData['StrikePriceDiffPer'], mergedData['StrikePriceDiffPerAbs'], \
            mergedData['LotSize'], mergedData['Premium'], mergedData['SpotAdjPremium'], \
            mergedData['OpIntradayChange'], mergedData['CmpIntradayChange'] = \
            zip(*mergedData.apply(self.getComputedData, axis=1))

    def computeFuturesData(self, mergedData):
        mergedData['Fwd'], mergedData['PriceDiff'], mergedData['PriceDiffPer'], mergedData['PriceDiffPerAbs'], \
            mergedData['FutureIntradayChange'], mergedData['CmpIntradayChange'] = \
                zip(*mergedData.apply(self.getFuturesComputedData, axis=1))

    def getFuturesComputedData(self, contract):
        cmp = contract['CMP']
        futPrice = contract['CLOSE_PRICE']

        fwd = ''
        if cmp == futPrice:
            fwd = 'SPOT'
        else:
            if futPrice > cmp:
                fwd = 'Premium'
            else:
                fwd = 'Discount'

        priceDiff = futPrice - cmp
        priceDiffPer = round(priceDiff / cmp * 100, 2)
        priceDiffPerAbs = abs(priceDiffPer)
        futIntradayChange = contract['CLOSE_PRICE'] - contract['OPEN_PRICE']
        cmpIntradayChange = contract['CMP'] - contract['OPEN']
        
        return fwd, priceDiff, priceDiffPer, priceDiffPerAbs, futIntradayChange, cmpIntradayChange

    #def determineCategory(self, contract):
    def getComputedData(self, contract):
        cmp = contract['CMP']
        strikePrice = contract['STR_PRICE']
        optionType = contract['OPT_TYPE']
        closePrice = contract['CLOSE_PRICE']

        cat = ''
        if cmp == strikePrice:
            cat = 'ATM'
        else:
            if optionType.strip() == 'CE':
                if strikePrice > cmp:
                    cat = 'OTM'
                else:
                    cat = 'ITM'
            else:
                if strikePrice > cmp:
                    cat = 'ITM'
                else:
                    cat = 'OTM'
        
        strikePriceDiff = (strikePrice - cmp)
        strikePriceDiffPer = round((strikePrice - cmp) / cmp * 100, 2)
        strikePriceDiffPerAbs = abs(strikePriceDiffPer)
        lotSize = round(contract['TRD_QTY'] / contract['NO_OF_CONT'])
        premium = round(lotSize * contract['CLOSE_PRICE'], 0)
        opIntradayChange = contract['CLOSE_PRICE'] - contract['OPEN_PRICE']
        cmpIntradayChange = contract['CMP'] - contract['OPEN']

        if cat == 'OTM':
            spotAdjPremium = closePrice + abs(strikePriceDiff)
        else:
            spotAdjPremium = closePrice - abs(strikePriceDiff)

        return cat, strikePriceDiff, strikePriceDiffPer, strikePriceDiffPerAbs, lotSize, premium, spotAdjPremium, opIntradayChange, cmpIntradayChange

    def filterOptions(self, filter, opt):
        options = opt
        if 'openIntThreshold' in filter:
            options = options[options['OPEN_INT*_x'] >= filter['openIntThreshold']]
        if 'optTypes' in filter:
            options = options[options.OPT_TYPE.isin(filter['optTypes'])]
        if 'strikePriceThreshold' in filter:
            options = options[options.StrikePriceDiffPerAbs >= filter['strikePriceThreshold']]
        if 'moneyness' in filter:
            options = options[options.Moneyness.isin(filter['moneyness'])]    
        if 'moneyness_x' in filter:
            options = options[options.Moneyness_x.isin(filter['moneyness_x'])]
        if 'strikePrice' in filter:
            options = options[options.STR_PRICE==filter['strikePrice']]
        if 'strikePrices' in filter:
            options = options[options.STR_PRICE.isin(filter['strikePrices'])]
        if 'prefExpiry' in filter:
            options = options[options.EXP_DATE==filter['prefExpiry']]
        if 'prefExpiries' in filter:
            options = options[options.EXP_DATE.isin(filter['prefExpiries'])]
        if 'symbol' in filter:
            options = options[options.SYMBOL==filter['symbol']]
        if '52weekLowThreshold' in filter:
            options = options[options['% above 52w low']<=filter['52weekLowThreshold']]
        if 'ForDate' in filter:
            options = options[options['ForDate']==filter['ForDate']]
        if 'sortBy' in filter:
            if 'sortAscending' in filter:
              options = options.sort_values(filter['sortBy'], ascending=filter['sortAscending'])
            else:
              options = options.sort_values(filter['sortBy'], ascending=False)
        
        if 'returnFullset' in filter:
            return options
        else:
            return options[self.optionsDisplayColumns]

    def filterComputedFutures(self, filter, fut):
        futures = fut
        if 'Fwd' in filter:
            futures = futures[futures.Fwd.isin(filter['Fwd'])]
        if 'symbol' in filter:
            futures = futures[futures.SYMBOL==filter['SYMBOL']]
        if 'deviation_range' in filter:
            deviation_range = filter['deviation_range']
            futures = futures[(futures.PriceDiffPer.between(deviation_range[0], deviation_range[1]))]
        if 'for_date_range' in filter:
            for_date_range = filter['for_date_range']
            futures = futures[(futures.ForDate_y.between(for_date_range[0], for_date_range[1]))]
        if 'sortBy' in filter:
            futures = futures.sort_values(filter['sortBy'], ascending=False)
        return futures

    def filterFutures(self, filter, fut):
        futures = fut
        if 'openIntThreshold' in filter:
            futures = futures[futures['OPEN_INT*'] >= filter['openIntThreshold']]
        if 'Fwd' in filter:
            futures = futures[futures.fwd.isin(filter['Fwd'])]
        if 'prefExpiry' in filter:
            futures = futures[futures.EXP_DATE >= filter['prefExpiry']]    
        if 'symbol' in filter:
            futures = futures[futures.SYMBOL==filter['symbol']]
        if 'sortBy' in filter:
            futures = futures.sort_values(filter['sortBy'], ascending=False)
        return futures[self.futDisplayCols]

    def computeOldAndNewVariances(self, newData, oldData):
        oldData['NewSpotPrice'], oldData['SpotPriceGrowth'], oldData['SpotPriceGrowthPer'], \
            oldData['NewPrice'], oldData['PriceGrowth'], oldData['PriceGrowthPer'], \
            oldData['NewPremium'], oldData['PremiumGrowth'], oldData['PremiumGrowthPer'] = \
            zip(*oldData.apply(self.getVarianceComputation, axis=1, newData=newData))

    def getDataVariances(self, newContract, oldContract, datapoint):
        newValue = newContract[datapoint].iloc[0]
        oldValue = oldContract[datapoint]
        growth = newValue - oldValue
        growthPer = round(growth / oldValue * 100, 1)
        return (newValue, growth, growthPer)

    def getVarianceComputation(self, contract, newData):
        #print(type(oldData))
        #return

        symbol = contract['SYMBOL']
        strikePrice = contract['STR_PRICE']
        optionType = contract['OPT_TYPE']
        expiry = contract['EXP_DATE']
        newContract = newData.loc[(newData['SYMBOL'] == symbol) & \
            (newData['STR_PRICE'] == strikePrice) & \
            (newData['OPT_TYPE'] == optionType) & \
            (newData['EXP_DATE'] == expiry) ]
        
        #print(oldContract['CMP']))

        if newContract.shape[0] > 0:
            spotPriceVariances = self.getDataVariances(newContract, contract, 'CMP')
            priceVariances = self.getDataVariances(newContract, contract, 'CLOSE_PRICE')
            premiumVariances = self.getDataVariances(newContract, contract, 'Premium')
            
            return spotPriceVariances[0], spotPriceVariances[1], spotPriceVariances[2], \
                priceVariances[0], priceVariances[1], priceVariances[2], \
                premiumVariances[0], premiumVariances[1], premiumVariances[2]
        else:
            return 0, 0, 0, 0, 0, 0, 0, 0, 0

    def getOptionsForDate(self, date, bhavCopy=None):
        options = self.reader.readOptionsData(date)
        if bhavCopy is not None:
            options = self.mergeBhavAndOp(options, bhavCopy)
            self.computeContractData(options)
        return options

    def getFuturesForDate(self, date, bhavCopy=None):
        futures = self.reader.readFuturesData(date)
        #futures['ExpiryTimeframe'] = 'Current' if futures['EXP_DATE'] == min(futures['EXP_DATE']) else ''
        if bhavCopy is not None:
            futures = self.mergeBhavAndFutures(futures, bhavCopy)
            self.computeFuturesData(futures)
        return futures

    def getFuturesForDateRange(self, start_date, end_date):
        
        current_date = start_date

        futures_data_list = []
        bhav_data_list = []

        http_error_dates = []
        other_error_dates = []

        while (end_date - current_date).days > 0:
            if (self.reader.isDataAvailableForDate(current_date)):
                try:
                    bhav_current_date = self.reader.readBhavcopy2(current_date)
                    bhav_data_list.append(bhav_current_date)
                    
                    futures_current_date = self.getFuturesForDate(current_date, bhav_current_date)
                    futures_data_list.append(futures_current_date)
                except HTTPError:
                    http_error_dates.append(current_date)
                    print('Http Error for date ' + current_date.strftime('date(%Y, %m, %d),'))
                except:
                    other_error_dates.append(current_date)
                    print('Error for date ' + current_date.strftime('date(%Y, %m, %d),'))
                    
            current_date = current_date + timedelta(days=1)
        return futures_data_list, bhav_data_list, http_error_dates, other_error_dates

    def getFuturesForDates(self, dates):
        futures_data_list = []
        bhav_data_list = []

        http_error_dates = []
        other_error_dates = []

        for current_date in dates:
            if (self.reader.isDataAvailableForDate(current_date)):
                try:
                    bhav_current_date = self.reader.readBhavcopy2(current_date)
                    bhav_data_list.append(bhav_current_date)
                    
                    futures_current_date = self.getFuturesForDate(current_date, bhav_current_date)
                    futures_data_list.append(futures_current_date)
                except HTTPError:
                    http_error_dates.append(current_date)
                    print('Http Error for date ' + current_date.strftime('date(%Y, %m, %d),'))
                except:
                    other_error_dates.append(current_date)
                    print('Error for date ' + current_date.strftime('date(%Y, %m, %d),'))
                    
            current_date = current_date + timedelta(days=1)
        return futures_data_list

    def compareFutures(self, fut1, fut2):
        comparison = pd.merge(fut1, fut2, how='inner', left_on=['SYMBOL', 'EXP_DATE'], right_on=['SYMBOL', 'EXP_DATE'])
        comparison['OiChange'] = comparison['OPEN_INT*_x'] - comparison['OPEN_INT*_y']
        comparison['OiChangePer'] = comparison['OiChange'] / comparison['OPEN_INT*_y'] * 100
        comparison['OiChangePerAbs'] = abs(comparison['OiChangePer'])
        return comparison[['SYMBOL', 'EXP_DATE', 'CLOSE_PRICE_x', 'CLOSE_PRICE_y', 'Fwd_x', 'Fwd_y', \
            'OiChangePer', 'OiChangePerAbs', 'CmpIntradayChange_x', 'FutureIntradayChange_x', 'OPEN_INT*_x', 'OPEN_INT*_y']]
        #return comparison

    def compareOptions(self, op1, op2):
        comparison = pd.merge(op1, op2, how='inner', left_on=['SYMBOL', 'STR_PRICE', 'EXP_DATE', 'OPT_TYPE'], right_on=['SYMBOL', 'STR_PRICE', 'EXP_DATE', 'OPT_TYPE'])
        comparison['OiChange'] = comparison['OPEN_INT*_x'] - comparison['OPEN_INT*_y']
        comparison['OiChangePer'] = comparison['OiChange'] / comparison['OPEN_INT*_y'] * 100
        comparison['OiChangePerAbs'] = abs(comparison['OiChangePer'])
        return comparison[['SYMBOL', 'STR_PRICE', 'EXP_DATE', 'OPT_TYPE', \
            'CLOSE_PRICE_x', 'CLOSE_PRICE_y', 'OPEN_INT*_x', 'OPEN_INT*_y', \
            'Premium_x', 'Moneyness_x', \
            'OiChange', 'OiChangePer', 'OiChangePerAbs']]

    def mergeEquityBhavAndDelivery(self, bhav, delivery):
        mergedData = pd.merge(delivery, bhav, how='inner', left_on=['SYMBOL', 'ForDate'], right_on=['SYMBOL', 'ForDate'])
        mergedData.rename(columns={'CLOSE': 'CMP'}, inplace=True)
        return mergedData

class SpreadStrategyCheck:
    NeutralSpread = None
    BullBiasSpread = None
    BearBiasSpread = None

    HighGrowthSelection = None
    HighCreditSelection = None

    KeyColumns = None

    def __init__(self):
        self.KeyColumns = ['SYMBOL', 'OPT_TYPE', 'EXP_DATE', 'LotSize', 'STR_PRICE', 'CMP', \
              'Moneyness', 'CLOSE_PRICE', 'NewPrice', 'Premium', 'PremiumGrowth', 'PremiumGrowthPer', \
             'SpotPriceGrowthPer', 'PriceGrowthPer', 'StrikePriceDiff', 'StrikePriceDiffPer', 'SpotAdjPremium']

        self.NeutralSpread = SpreadParams(['ITM'], ['ITM', 'OTM'], \
            unhedgedCEPer = 0.05, unhedgedPEPer=0.05, itmThresholdCEPer=0.1, itmThresholdPEPer=0.1)
        self.BullBiasSpread = SpreadParams(['ITM'], ['ITM', 'OTM'], \
            unhedgedCEPer = 0.05, unhedgedPEPer=0.05, itmThresholdCEPer=0.1, itmThresholdPEPer=0.15)
        self.BearBiasSpread = SpreadParams(['ITM'], ['ITM', 'OTM'], \
            unhedgedCEPer = 0.05, unhedgedPEPer=0.05, itmThresholdCEPer=0.15, itmThresholdPEPer=0.1)


        self.HighCreditSelection = SelectionParams('Premium', highestForSell=True, highestForBuy=False)
        self.HighGrowthSelection = SelectionParams('PremiumGrowth', highestForSell=False, highestForBuy=True)
        

    def filterContracts(self, compiled, expLimits):
        contracts = compiled.old[(compiled.old['SYMBOL'] == 'NIFTY') \
                                & (compiled.old['EXP_DATE'].isin(expLimits)) \
                                & (compiled.old['CLOSE_PRICE'] > 0) & (compiled.old['NewPrice'] > 0) \
                            & (compiled.old['OPEN_INT*'] >= 100)][self.KeyColumns]
        return contracts


    def getContract(self, contracts, optType, categoriesLimit, strikeLow, strikeHigh, sortBy, sortByAsc):
        print(strikeLow, strikeHigh)

        contract = contracts[(contracts['OPT_TYPE'] == optType) & \
                        (contracts['STR_PRICE'] <= strikeHigh) & (contracts['STR_PRICE'] >= strikeLow) \
                        & (contracts['Moneyness'].isin(categoriesLimit))\
                        ].sort_values(sortBy, ascending=sortByAsc).iloc[0]
        return contract


    def bestSpread(self, compiled, cmp, spreadParams, selectionParams, expLimits):
        unhedgedPE = cmp - (cmp * spreadParams.UnhedgedPEPer)
        unhedgedCE = cmp + (cmp * spreadParams.UnhedgedCEPer)
        itmThresholdCE = cmp - (cmp * spreadParams.ItmThresholdCEPer)
        itmThresholdPE = cmp + (cmp * spreadParams.ItmThresholdPEPer)
        
        contracts = self.filterContracts(compiled, expLimits)
        
        sellPE = self.getContract(contracts, 'PE', spreadParams.SellCategories, cmp, itmThresholdPE, selectionParams.SortBy, not selectionParams.HighestForSell)
        buyPE = self.getContract(contracts, 'PE', spreadParams.BuyCategories, unhedgedPE, cmp, selectionParams.SortBy, not selectionParams.HighestForBuy)
        sellCE = self.getContract(contracts, 'CE', spreadParams.SellCategories, itmThresholdCE, cmp, selectionParams.SortBy, not selectionParams.HighestForSell)
        buyCE = self.getContract(contracts, 'CE', spreadParams.BuyCategories, cmp, unhedgedCE, selectionParams.SortBy, not selectionParams.HighestForBuy)
    
    
        profit = (sellPE.PremiumGrowth * -1) + buyPE.PremiumGrowth + (sellCE.PremiumGrowth * -1) + buyCE.PremiumGrowth
        strategy = pd.DataFrame([sellPE, buyPE, sellCE, buyCE])
        
        display(strategy)
        print('Profit: ' + str(round(profit, 2)))

class SelectionParams:
    SortBy: None
    HighestForSell: None
    HighestForBuy: None

    def __init__(self, sortBy, highestForSell, highestForBuy):
        self.SortBy = sortBy
        self.HighestForSell = highestForSell
        self.HighestForBuy = highestForBuy

class SpreadParams:
    SellCategories = None
    BuyCategories = None
    UnhedgedCEPer = None
    UnhedgedPEPer = None
    ItmThresholdCEPer = None
    ItmThresholdPEPer = None

    def __init__(self, sellCategories, buyCategories, unhedgedCEPer, unhedgedPEPer, itmThresholdCEPer, itmThresholdPEPer):
        self.SellCategories = sellCategories
        self.BuyCategories = buyCategories
        self.UnhedgedCEPer = unhedgedCEPer
        self.UnhedgedPEPer = unhedgedPEPer
        self.ItmThresholdCEPer = itmThresholdCEPer
        self.ItmThresholdPEPer = itmThresholdPEPer

class DataVisualizer:
    compiledData: None

    def __init__(self, compiledData):
        self.compiledData = compiledData

    def boxplot(self, x, y, title, xlabel, ylabel, ylim_min, ylim_max):
        plt.figure(figsize=(12,6))
        plot = sns.boxplot(x=x, y=y, data=self.compiledData.new, fliersize=2, notch=True).set_ylim(ylim_min, ylim_max)
        plt.title(title)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.show()
        return plot

class OiParticipantsVisualizer:
    oi_datapoints: None

    def init(self):
        self.oi_datapoints = {
            'pairs':[]
        }

        self.add_oi_datapoint('Future Index')
        self.add_oi_datapoint('Option Index Call')
        self.add_oi_datapoint('Option Index Put', bullishPosition = False)
        self.add_oi_datapoint('Future Stock')
        self.add_oi_datapoint('Option Stock Call')
        self.add_oi_datapoint('Option Stock Put', bullishPosition = False)
            
    def add_delta_datapoints(self, data):
        for pair in self.oi_datapoints['pairs']:
            data[pair['name'] +' Delta'] = data[pair['name'] + ' Long'] - data[pair['name'] + ' Short']
        return data

    #def add_oi_datapoint(name, dp1, dp2):
    def add_oi_datapoint(self, name, bullishPosition = True):
        longSignal, shortSignal, deltaSignal = 'bullish', 'bearish', 'delta'
        
        if bullishPosition == False:
            longSignal, shortSignal = 'bearish', 'bullish'
        
        self.oi_datapoints['pairs'].append({
            'name': name, 
            'dp': [
                {'name': name + ' Long', 'signal': longSignal}, 
                {'name': name + ' Short', 'signal': shortSignal},
                {'name': name + ' Delta', 'signal': deltaSignal}
            ]
        })
    
    def renderChart(self, oi, col, color, title):
        sns.light_palette("seagreen", as_cmap=True)
        chart = sns.lineplot(data=oi, x='ForDate', y=col, legend="full", palette=color, hue="Client Type", markers=True)
        chart.set_title(title)
        
    def oiCharts(self, oi, clientTypes=['FII'], months = None):
        if clientTypes is not None:
            filtered = oi[oi['Client Type'].isin(clientTypes)]
        else:
            filtered = oi[oi['Client Type'] != 'TOTAL']
        
        if months is not None:
            filtered = filtered[filtered['ForDate'].dt.month.isin(months)]
        
        signalPalette = {
            'bullish': 'Greens',
            'bearish': 'Reds'
        }
        
        for pair in self.oi_datapoints['pairs']:
        #for pair in [oi_datapoints['pairs'][0]]:
            fig, ax = plt.subplots()
            fig.set_size_inches(18, 4)
            self.renderChart(filtered, pair['dp'][0]['name'], signalPalette[pair['dp'][0]['signal']], pair['name'])
            self.renderChart(filtered, pair['dp'][1]['name'], signalPalette[pair['dp'][1]['signal']], pair['name'])
            sns.despine()

        return filtered

class EquityDeliveryVisualizer:
    def renderDeliveryChart(self, data, datapoint):
        fig, ax = plt.subplots()
        fig.set_size_inches(18, 4)
        sns.lineplot(data=data, x='ForDate', y=datapoint, legend="full", markers=True)
        sns.despine()

    def renderPriceChart(self, data):
        fig, ax = plt.subplots()
        fig.set_size_inches(18, 4)
        sns.lineplot(data=data, x='ForDate', y='CMP', legend="full", markers=True)
        sns.despine()

    def showDeliveryChartsForSymbols(self, symbols, data):
        grp_data = data[(data['SYMBOL'].isin(symbols)) & (data['RecordType'] == 'EQ')]
        grp_data = grp_data.groupby('ForDate').sum().reset_index()
        self.renderDeliveryChart(grp_data, datapoint='DeliverableQuantity')
    
    def showDeliveryPercChartsForSymbols(self, symbols, data):
        grp_data = data[(data['SYMBOL'].isin(symbols)) & (data['RecordType'] == 'EQ')]
        grp_data = grp_data.groupby('ForDate').mean().reset_index()
        self.renderDeliveryChart(grp_data, datapoint='DeliverableQuantityPercent')

class FuturesDataVisualizer:
    SelectColumns = ['SYMBOL', 'EXP_DATE', 'CLOSE_PRICE', 'CMP', 'OPEN_INT*', 'ForDate_y', 'PriceDiffPer', 'PriceDiff', 'OPEN', 'HIGH', 'LOW']

    def show_highly_deviated_stocks(self, futures_data, bhav_data_all, fwd_match = 'Discount', show_chart=True, top_n_charts = 10, chart_prev_days=10, chart_next_days=10):
        if fwd_match == 'Discount':
            idx = futures_data.groupby(['SYMBOL', 'EXP_DATE'])['PriceDiffPer'].transform(min) == futures_data['PriceDiffPer']
        else:
            idx = futures_data.groupby(['SYMBOL', 'EXP_DATE'])['PriceDiffPer'].transform(max) == futures_data['PriceDiffPer']

        highly_deviated_stocks = futures_data[idx].sort_values('PriceDiffPer')[self.SelectColumns]

        if show_chart == True:
            for index, row in highly_deviated_stocks.head(top_n_charts).iterrows():
                event_date = row.ForDate_y
                event_text = 'Highest ' + fwd_match + ' date'
                from_date = event_date - timedelta(days=chart_prev_days)
                to_date = event_date + timedelta(days=chart_next_days)
                df = bhav_data_all[ (bhav_data_all.SYMBOL == row.SYMBOL) &
                    bhav_data_all.ForDate.between(from_date, to_date)]
                fig = go.Figure(data=[go.Candlestick(x=df['ForDate'],
                    open=df['OPEN'],
                    high=df['HIGH'],
                    low=df['LOW'],
                    close=df['CMP'],
                    xhoverformat='%a, %b %d, %Y',
                )])
                fig.update_layout(
                    yaxis_tickformat = 'd',
                    margin=dict(l=20, r=20, t=50, b=50),
                    height=200,
                    font_size=10
                )
                fig.update_xaxes(rangeslider_visible=False)

                shapes = []
                annotations = []
                shapes.append(dict(
                    x0=event_date, x1=event_date, y0=0, y1=1, xref='x', yref='paper',
                    opacity=0.1,
                    line_width=10))
                annotations.append(dict(
                    x=event_date, y=0.05, xref='x', yref='paper',
                    showarrow=False, xanchor='left', text=event_text))

                fig.update_layout(
                    title = {
                    'text': str(row.PriceDiffPer) + '% ' + fwd_match + ' for ' + row.SYMBOL + ' on ' + event_date.strftime("%a, %d %b, %Y"),
                    'y':0.9,
                    'x':0.5,
                    'xanchor': 'center',
                    'yanchor': 'top',
                    'font': dict(size=12)
                    },
                    shapes=shapes,
                    annotations=annotations
                )

                fig.show()
        
        return highly_deviated_stocks