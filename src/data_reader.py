from urllib.request import urlopen
from zipfile import ZipFile
from environment import EnvironmentSettings
import os
from string import Template
import pandas as pd
from urllib.error import HTTPError

from core import MarketDaysHelper

class ReaderOptions:
  filename = Template("")
  url_template = Template("")
  output_path_template = Template("")
  unzip_path_template = Template("")
  primary_data_path_template = Template("")
  unzip_file = True

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
      print(f"Downloading data for {for_date}")
      url = self.options.url_template.substitute( **({**date_parts, **filenames}) )

      print(url)
      
      urldata = urlopen(url, timeout=self.options.download_timeout)
      with open(output_file_path,'wb') as output:
        output.write(urldata.read())

    unzip_folder_path = self.options.unzip_path_template.substitute( **({**EnvironmentSettings.Paths, **filenames}) )
    if self.options.unzip_file == True and not os.path.exists(unzip_folder_path):
      self.unzip_content(output_file_path, unzip_folder_path, for_date)

    #print(primary_data_file_path)
    
    return self.read_data_from_file(for_date, primary_data_file_path)

  def unzip_content(self, output_file_path, unzip_folder_path, for_date):
    zf = ZipFile(output_file_path)
    zf.extractall(path=unzip_folder_path)
    zf.close()

  def read_data_from_file(self, for_date, primary_data_filepath):
    primary_data = pd.read_csv(primary_data_filepath)
    primary_data['Date'] = for_date
    return primary_data

  def get_filenames(self, for_date):
    return Exception("Not implemented!")

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

class NseIndicesReader(DataReader):
  def __init__(self):
    super().__init__()
    self.name = "Indices"
    __base_url = "https://archives.nseindia.com/content/indices/"
    self.options.unzip_file = False
    self.options.url_template = Template(__base_url + "$download_filename")
    self.options.output_path_template = Template("$DataBaseDir/$RawDataDir/$NseIndicesDataDir/$download_filename")
    self.options.primary_data_path_template = Template("$DataBaseDir/$RawDataDir/$NseIndicesDataDir/$download_filename")

  def get_filenames(self, for_date):
    __formatted_date = for_date.strftime('%d%m%Y').upper()
    return {
      'download_filename': f"ind_close_all_{__formatted_date}.csv",
      'primary_data_filename':  f"ind_close_all_{__formatted_date}.csv"
    }

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
                  result = pd.concat([result, data], ignore_index=True)
            except HTTPError as e:
              print(e, for_date)
    return result