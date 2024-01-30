import math
import markets_insights as mi

from urllib.request import urlopen
from zipfile import ZipFile
from markets_insights.core.column_definition import BaseColumns, BasePriceColumns, DerivativesBaseColumns
from markets_insights.core.environment import EnvironmentSettings
import os
from string import Template
import pandas as pd
from urllib.error import HTTPError
from datetime import date

from markets_insights.core.core import FilterBase, MarketDaysHelper, Instrumentation, TypeHelper

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
    self.volume_scale: int = 1
    self.turnover_scale: int = 1
    self.filter = None
    self.name = ""
    self.col_prefix = None

  def set_filter(self, filter: FilterBase):
    self.filter = filter
    return self

  def get_date_parts(self, for_date):
    return {
      'year': str(for_date.year),
      'month': for_date.strftime("%B").upper()[:3],
      'day': str(for_date.strftime("%d"))
    }

  def merge_with_operation_on_price(self, data_l: pd.DataFrame, data_r: pd.DataFrame, operation) -> pd.DataFrame:
    merged_df = pd.merge(data_l, data_r, how='inner', 
                         on=[BaseColumns.Identifier, BaseColumns.Date], left_index=False, right_index=False)
    
    for col in TypeHelper.get_class_static_values(BasePriceColumns):
      merged_df[col] = operation(merged_df(merged_df[col+'_x'], merged_df[col+'_y']))
    
    return merged_df

  def read(self, for_date) -> pd.DataFrame:
    return self.read_data(for_date)
  
  def read_data(self, for_date) -> pd.DataFrame:
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

    data = self.read_data_from_file(for_date, primary_data_file_path)

    column_name_mappings = self.get_column_name_mappings()
    if column_name_mappings is not None:
      data.rename(columns = column_name_mappings, inplace = True)

    data.drop_duplicates(inplace=True)

    self.normalise_base_column_values(data)
    
    if self.filter:
      data = data.query(str(self.filter))

    return self.sanitize_data(data)

  def normalise_base_column_values(self, data: pd.DataFrame) -> pd.DataFrame:
    for col_name in [BaseColumns.Open, BaseColumns.High, BaseColumns.Low]:
      data[col_name] = data.apply(lambda x: x[col_name] if str(x[col_name]).replace(".", "").isnumeric() == True else x[BaseColumns.Close], axis=1)
      data[col_name] = data[col_name].astype(float)

    for col_name in [BaseColumns.Volume, BaseColumns.Turnover]:
      if col_name in data.columns:
        data[col_name] = data[col_name].apply(lambda val: val if str(val).replace(".", "").isnumeric() == True else math.nan).astype(float)

    for rescaling in [(BaseColumns.Volume, self.volume_scale), (BaseColumns.Turnover, self.turnover_scale)]:
      if rescaling[1] != 1:
        data[rescaling[0]] = data[rescaling[0]] * rescaling[1]

    return data

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
  
  def sanitize_data(self, data):
    return data

  def __sub__(self, other):
    return ArithmaticOpReader(left = self, right = other, operator = lambda x, y: x - y, op_symbol = "-")

  def __add__(self, other):
    return ArithmaticOpReader(left = self, right = other, operator = lambda x, y: x + y, op_symbol = "+")
  
  def __mul__(self, other):
    return ArithmaticOpReader(left = self, right = other, operator = lambda x, y: x * y, op_symbol = "x")
  
  def __truediv__(self, other):
    return ArithmaticOpReader(left = self, right = other, operator = lambda x, y: x / y, op_symbol = " div ")

class ArithmaticOpReader(DataReader):
  def __init__(self, left: DataReader, right: DataReader, operator, op_symbol: str):
    super().__init__()
    self.l_reader = left
    self.r_reader = right
    self.operator = operator
    self.op_symbol = op_symbol
    self.col_prefix = ""
    self.name = f"{left.name}{op_symbol}{right.name}"
  
  def read(self, for_date: date) -> pd.DataFrame:
    l_data = self.l_reader.read(for_date = for_date)
    r_data = self.r_reader.read(for_date = for_date)

    on_cols = None
    prefix_l = None
    prefix_r = None
    
    rename_id_col = False
    if len(l_data[BaseColumns.Identifier].unique()) == 1:
      on_cols = [BaseColumns.Date]
      rename_id_col = True
      prefix_l = l_data[BaseColumns.Identifier].values[0]+"-"
      prefix_r = self.r_reader.col_prefix
    elif len(r_data[BaseColumns.Identifier].unique()) == 1:
      on_cols = [BaseColumns.Date]
      rename_id_col = True
      prefix_l = self.l_reader.col_prefix
      prefix_r = r_data[BaseColumns.Identifier].values[0]+"-"
    else:
      on_cols = [BaseColumns.Identifier, BaseColumns.Date]
      rename_id_col = False
      prefix_l = self.l_reader.col_prefix
      prefix_r = self.r_reader.col_prefix

    if not (f"{prefix_l}{BaseColumns.Close}" in l_data.columns and f"{prefix_r}{BaseColumns.Close}" in l_data.columns):
      merged_df = pd.merge(l_data, r_data, how='inner', on=on_cols, 
                            left_index=False, right_index=False)
      if rename_id_col:
        merged_df.rename(columns={f"{BaseColumns.Identifier}_x": BaseColumns.Identifier}, inplace=True)
      
      update_col_prefix = {}
      for col in [col for col in merged_df.columns if '_x' in col]:
        update_col_prefix[col] = f"{prefix_l}{col.replace('_x', '')}"
      for col in [col for col in merged_df.columns if '_y' in col]:
        update_col_prefix[col] = f"{prefix_r}{col.replace('_y', '')}"
      
      merged_df.rename(columns=update_col_prefix, inplace=True)
    else:
      merged_df = l_data
      
    for col in TypeHelper.get_class_static_values(BasePriceColumns):
      Instrumentation.info(f"{prefix_l}{col} {self.op_symbol} {prefix_r}{col}")
      merged_df[col] = self.operator(merged_df[f"{prefix_l}{col}"], merged_df[f"{prefix_r}{col}"])
    
    return merged_df

class BhavCopyReader(DataReader):
  def __init__(self):
    super().__init__()
    self.name = "nse_equities"
    self.col_prefix = "Cash-"
    __base_url = "https://archives.nseindia.com/content/historical/EQUITIES/"
    self.options.url_template = Template(__base_url + "$year/$month/$download_filename")
    self.options.output_path_template = Template(f"$DataBaseDir/$RawDataDir/$BhavDataDir/$download_filename")
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
      'SYMBOL': BaseColumns.Identifier,
      'PREVCLOSE': BaseColumns.PreviousClose,
      'OPEN': BaseColumns.Open,
      'HIGH': BaseColumns.High,
      'LOW': BaseColumns.Low,
      'CLOSE': BaseColumns.Close,
      'TOTTRDQTY': BaseColumns.Volume,
      'TOTTRDVAL': BaseColumns.Turnover
    }

  def sanitize_data(self, data):
    return data[data['SERIES'] == 'EQ'].reset_index(drop=True)

class NseIndicesReader(DataReader):
  def __init__(self):
    super().__init__()
    self.name = "Indices"
    self.col_prefix = "Index-"
    self.turnover_scale = math.pow(10, 7)
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
      'Index Name': BaseColumns.Identifier,
      'Open Index Value': BaseColumns.Open,
      'High Index Value': BaseColumns.High,
      'Low Index Value': BaseColumns.Low,
      'Closing Index Value': BaseColumns.Close,
      'Volume': BaseColumns.Volume,
      'Turnover (Rs. Cr.)': BaseColumns.Turnover
    }

class NseDerivatiesReaderBase(DataReader):
  def sanitize_data(self, data):
    return data[data['OpenInterest'] > 0].reset_index(drop=True)

class NseDerivatiesReader(NseDerivatiesReaderBase):
  def __init__(self):
    super().__init__()
    self.name = "Derivatives"
    self.col_prefix = "FO-"
    self.turnover_scale = math.pow(10, 7)
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
      'TckrSymb': BaseColumns.Identifier,
      'XpryDt': DerivativesBaseColumns.ExpiryDate,
      'OptnTp': DerivativesBaseColumns.OptionType,
      'PrvsClsgPric': BaseColumns.PreviousClose,
      'TtlTradgVol': BaseColumns.Volume,
      'TtlTrfVal': BaseColumns.Turnover,
      'OpnPric': BaseColumns.Open,
      'HghPric': BaseColumns.High,
      'LwPric': BaseColumns.Low,
      'ClsPric': BaseColumns.Close,
      'OpnIntrst': DerivativesBaseColumns.OpenInterest,
      'PctgChngInOpnIntrst': DerivativesBaseColumns.OiChangePct,
      'StrkPric': DerivativesBaseColumns.StrikePrice,
      'FinInstrmNm': DerivativesBaseColumns.InstrumentType
    }

class NseDerivatiesOldReader(NseDerivatiesReaderBase):
  def __init__(self):
    super().__init__()
    self.name = "Derivatives"
    self.col_prefix = "FO-"
    self.turnover_scale = math.pow(10, 7)
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
      'SYMBOL': BaseColumns.Identifier,
      'EXPIRY_DT': DerivativesBaseColumns.ExpiryDate,
      'OPTION_TYP': DerivativesBaseColumns.OptionType,
      'PrvsClsgPric': BaseColumns.PreviousClose,
      'TtlTradgVol': BaseColumns.Turnover,
      'OPEN': BaseColumns.Open,
      'HIGH': BaseColumns.High,
      'LOW': BaseColumns.Low,
      'CLOSE': BaseColumns.Close,
      'OpnIntrst': DerivativesBaseColumns.OpenInterest,
      'PctgChngInOpnIntrst': DerivativesBaseColumns.OiChangePct,
      'STRIKE_PR': DerivativesBaseColumns.StrikePrice
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
  
class DateRangeDataReader (DataReader):
  reader: DataReader

  def __init__(self, reader: DataReader):
    super().__init__()
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

