from urllib.request import urlopen
from zipfile import ZipFile
from core.environment import EnvironmentSettings
import os
from string import Template
import pandas as pd
from urllib.error import HTTPError
from datetime import date

from core.core import MarketDaysHelper, Instrumentation

class ReaderOptions:
  filename = Template("")
  url_template = Template("")
  output_path_template = Template("")
  unzip_path_template = Template("")
  primary_data_path_template = Template("")
  unzip_file = True
  cutoff_date = None

  download_timeout = 5 #seconds
   
class DataReader:
  options: ReaderOptions

  def __init__(self):
    self.options = ReaderOptions()
    self.name = ""

  def get_date_parts(self, for_date):
    return {
      'year': str(for_date.year),
      'month': for_date.strftime("%B").upper()[:3],
      'day': str(for_date.strftime("%d"))
    }
  
  def get_data(self, for_date):
    return self.download_data(for_date)

  def read(self, for_date):
    date_parts = self.get_date_parts(for_date)
    filenames = self.get_filenames(for_date)
    output_file_path = self.options.output_path_template.substitute( **({**EnvironmentSettings.Paths, **filenames}) )
    primary_data_file_path = self.options.primary_data_path_template.substitute( **({**EnvironmentSettings.Paths, **filenames}) )

    if not os.path.exists(output_file_path):
      Instrumentation.debug(f"Downloading data for {for_date}")
      url = self.options.url_template.substitute( **({**date_parts, **filenames}) )

      Instrumentation.debug(url)
      
      urldata = urlopen(url, timeout=self.options.download_timeout)
      with open(output_file_path,'wb') as output:
        output.write(urldata.read())

    unzip_folder_path = self.options.unzip_path_template.substitute( **({**EnvironmentSettings.Paths, **filenames}) )
    if self.options.unzip_file == True and not os.path.exists(unzip_folder_path):
      self.unzip_content(output_file_path, unzip_folder_path, for_date)

    #print(primary_data_file_path)
    
    data = self.read_data_from_file(for_date, primary_data_file_path)

    column_name_mappings = self.get_column_name_mappings()
    if column_name_mappings is not None:
      data.rename(columns = column_name_mappings, inplace = True)

    data.drop_duplicates(inplace=True)

    return self.filter_data(data)

  def unzip_content(self, output_file_path, unzip_folder_path, for_date):
    zf = ZipFile(output_file_path)
    zf.extractall(path=unzip_folder_path)
    zf.close()

  def read_data_from_file(self, for_date, primary_data_filepath):
    primary_data = pd.read_csv(primary_data_filepath)
    primary_data['Date'] = pd.to_datetime(for_date)
    return primary_data

  def get_filenames(self, for_date):
    return Exception("Not implemented!")

  def get_column_name_mappings(self):
    return None
  
  def filter_data(self, data):
    return data

class BhavCopyReader(DataReader):
  def __init__(self):
    super().__init__()
    self.name = "Equities"
    __base_url = "https://archives.nseindia.com/content/historical/EQUITIES/"
    self.options.url_template = Template(__base_url + "$year/$month/$download_filename")
    self.options.output_path_template = Template("$DataBaseDir/$RawDataDir/$BhavDataDir/$download_filename")
    self.options.unzip_path_template = Template("$DataBaseDir/$RawDataDir/$BhavDataDir/$download_filename_wo_ext")
    self.options.primary_data_path_template = Template("$DataBaseDir/$RawDataDir/$BhavDataDir/$download_filename_wo_ext/$primary_data_filename")

  def get_filenames(self, for_date):
    __formatted_date = for_date.strftime('%d%b%Y').upper()
    return {
      'download_filename_wo_ext': f"cm{__formatted_date}bhav",
      'download_filename': f"cm{__formatted_date}bhav.csv.zip",
      'primary_data_filename':  f"cm{__formatted_date}bhav.csv"
    }
  
  def get_column_name_mappings(self):
    return {
      'SYMBOL': 'Identifier',
      'TOTTRDVAL': 'Turnover (Rs. Cr.)',
      'PREVCLOSE': 'PreviousClose',
      'OPEN': 'Open',
      'HIGH': 'High',
      'LOW': 'Low',
      'CLOSE': 'Close'
    }

  def filter_data(self, data):
    return data[data['SERIES'] == 'EQ'].reset_index(drop=True)

class NseIndicesReader(DataReader):
  def __init__(self):
    super().__init__()
    self.name = "Indices"
    __base_url = "https://archives.nseindia.com/content/indices/"
    self.options.unzip_file = False
    self.options.url_template = Template(__base_url + "$download_filename")
    self.options.output_path_template = Template("$DataBaseDir/$RawDataDir/$NseIndicesDataDir/$download_filename")
    self.options.primary_data_path_template = Template("$DataBaseDir/$RawDataDir/$NseIndicesDataDir/$download_filename")
    self.options.cutoff_date = date.fromisoformat("2013-01-01")

  def get_filenames(self, for_date):
    __formatted_date = for_date.strftime('%d%m%Y').upper()
    return {
      'download_filename': f"ind_close_all_{__formatted_date}.csv",
      'primary_data_filename':  f"ind_close_all_{__formatted_date}.csv"
    }
  
  def get_column_name_mappings(self):
    return {
      'Index Name': 'Identifier',
      'Open Index Value': 'Open',
      'High Index Value': 'High',
      'Low Index Value': 'Low',
      'Closing Index Value': 'Close'
    }

class NseDerivatiesReaderBase(DataReader):
  def filter_data(self, data):
    return data[data['OpenInterest'] > 0].reset_index(drop=True)

class NseDerivatiesReader(NseDerivatiesReaderBase):
  def __init__(self):
    super().__init__()
    self.name = "Derivatives"
    self.options.unzip_file = False
    __base_url = "https://archives.nseindia.com/content/fo/"
    self.options.url_template = Template(__base_url + "$download_filename")
    self.options.output_path_template = Template("$DataBaseDir/$RawDataDir/$NseDerivativesDataDir/$download_filename")
    self.options.primary_data_path_template = Template("$DataBaseDir/$RawDataDir/$NseDerivativesDataDir/$download_filename")
    
  def get_filenames(self, for_date):
    #__formatted_date = for_date.strftime('%d%b%Y').upper()
    __formatted_date = for_date.strftime('%d%m%Y')
    return {
      'download_filename': f"NSE_FO_bhavcopy_{__formatted_date}.csv",
      'primary_data_filename':  f"NSE_FO_bhavcopy_{__formatted_date}.csv"
    }
  
  def get_column_name_mappings(self):
    return {
      'TckrSymb': 'Identifier',
      'XpryDt': 'ExpiryDate',
      'OptnTp': 'OptionType',
      'PrvsClsgPric': 'PreviousClose',
      'TtlTradgVol': 'Turnover (Rs. Cr.)',
      'OpnPric': 'Open',
      'HghPric': 'High',
      'LwPric': 'Low',
      'ClsPric': 'Close',
      'OpnIntrst': 'OpenInterest',
      'PctgChngInOpnIntrst': 'OiChangePerc'
    }

class NseDerivatiesOldReader(NseDerivatiesReaderBase):
  def __init__(self):
    super().__init__()
    self.name = "Derivatives"
    __base_url = "https://archives.nseindia.com/content/historical/DERIVATIVES/"
    self.options.url_template = Template(__base_url + "$year/$month/$download_filename")
    self.options.output_path_template = Template("$DataBaseDir/$RawDataDir/$NseDerivativesDataDir/$download_filename")
    self.options.unzip_path_template = Template("$DataBaseDir/$RawDataDir/$NseDerivativesDataDir/$download_filename_wo_ext")
    self.options.primary_data_path_template = Template("$DataBaseDir/$RawDataDir/$NseDerivativesDataDir/$download_filename_wo_ext/$primary_data_filename")
    
  def get_filenames(self, for_date):
    __formatted_date = for_date.strftime('%d%b%Y').upper()
    return {
      'download_filename_wo_ext': f"fo{__formatted_date}bhav",
      'download_filename': f"fo{__formatted_date}bhav.csv.zip",
      'primary_data_filename':  f"fo{__formatted_date}bhav.csv"
    }
  
  def get_column_name_mappings(self):
    return {
      'SYMBOL': 'Identifier',
      'EXPIRY_DT': 'ExpiryDate',
      'OPTION_TYP': 'OptionType',
      'STRIKE_PR': 'StrkPric',
      'OPEN_INT': 'OpenInterest',
      'TtlTradgVol': 'Turnover (Rs. Cr.)',
      'OPEN': 'Open',
      'HIGH': 'High',
      'LOW': 'Low',
      'CLOSE': 'Close',
      'OpnIntrst': 'OpenInterest',
      'PctgChngInOpnIntrst': 'OiChangePerc'
    }

class MultiDatesDataReader:
  reader: DataReader

  def __init__(self, reader: DataReader):
    self.reader = reader
  
  def read(self, datelist):
    
    result = None
    for for_date in datelist:
        if MarketDaysHelper.is_open_for_day(pd.Timestamp(for_date).date()):
            try:
              data = self.reader.read(for_date)
              if result is None:
                  result = data
              else:
                  result = pd.concat([result, data], ignore_index=True).reset_index(drop=True)
            except Exception as e:
              print(e, for_date.strftime('date(%Y, %m, %d),'))
            
    return result
  
class DateRangeDataReader:
  reader: DataReader

  def __init__(self, reader: DataReader):
    self.reader = reader
  
  def read(self, from_date, to_date):
    
    datelist = MarketDaysHelper.get_days_list_for_range(from_date, to_date)
    result = None
    for for_date in datelist:
        if MarketDaysHelper.is_open_for_day(pd.Timestamp(for_date).date()):
            try:
              data = self.reader.read(for_date)
              if result is None:
                  result = data
              else:
                  result = pd.concat([result, data], ignore_index=True).reset_index(drop=True)
            except Exception as e:
              print(e, for_date.strftime('date(%Y, %m, %d),'))
            
    return result

