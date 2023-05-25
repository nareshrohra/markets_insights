from environment import EnvironmentSettings
import os
from string import Template
import pandas as pd
from data_reader import DataReader, DateRangeDataReader
from core import MarketDaysHelper

class DataProcessor:
  output_dir_template = Template("")
  output_file_template = Template("$reader_name.csv")
  options: dict

  def process(self, reader):
    return Exception("Not implemented!")
    
class HistoricalDataProcessor(DataProcessor):
  def __init__(self):
    self.output_dir_template = Template("$DataBaseDir/$ProcessedDataDir/$HistoricalDataDir")

  def process(self, reader: DataReader, options: dict):
    from_date = MarketDaysHelper.get_this_or_next_market_day(options['from_date'])
    to_date = MarketDaysHelper.get_this_or_previous_market_day(options['to_date'])

    output_file = os.path.join(self.output_dir_template.substitute(**EnvironmentSettings.Paths), 
                               self.output_file_template.substitute(**{'reader_name': reader.name}))
  
    dateRangeReader = DateRangeDataReader(reader)
    if os.path.exists(output_file):
      historical_data = pd.read_csv(output_file)
      earliest = pd.Timestamp(historical_data['Date'].min()).date()
      latest = pd.Timestamp(historical_data['Date'].max()).date()
      if earliest > from_date:
        print(f"Reading data from {from_date} to {earliest}")
        historical_data = pd.concat([historical_data, dateRangeReader.read(from_date, earliest)], ignore_index=True)
      
      if latest < to_date:
        print(f"Reading data from {latest} to {to_date}")
        historical_data = pd.concat([historical_data, dateRangeReader.read(latest, to_date)], ignore_index=True)

    else:
      historical_data = dateRangeReader.read(from_date, to_date)
    
    historical_data.to_csv(output_file)

    return historical_data