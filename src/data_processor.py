from environment import EnvironmentSettings
import os
from string import Template
import pandas as pd
from data_reader import DataReader, DateRangeDataReader
from core import MarketDaysHelper, Instrumentation
from datetime import date
import glob

class DataProcessor:
  output_dir_template = Template("")
  filename_template = Template("$ReaderName.csv")
  manual_data_dir_template = Template("$ManualDataPath")
  options: dict

  def process(self, reader: DataReader):
    return Exception("Not implemented!")
  
  def remove_unnamed_columns(self, data: pd.DataFrame):
    return data.loc[:, ~data.columns.str.match("Unnamed")]

class ManualDataProcessor(DataProcessor):

  @Instrumentation.trace(name="ManualDataProcessor.process")
  def process(self, reader: DataReader):
    dir_path = os.path.join(self.manual_data_dir_template.substitute(**EnvironmentSettings.Paths), reader.name)
    
    Instrumentation.debug(dir_path)
    csv_files = glob.glob(os.path.join(dir_path, '*.csv'))

    dfs = []
    for file in csv_files:
      df = pd.read_csv(file)
      
      filename = file.split(os.path.sep)[-1].split('.')[0]
      column_value = filename.replace('_Data', '')
      
      df['Identifier'] = column_value
      
      dfs.append(df)

    final_df = pd.concat(dfs)

    final_df['Date'] = pd.to_datetime(final_df['Date'])

    output_file = os.path.join(self.manual_data_dir_template.substitute(**EnvironmentSettings.Paths), 
                                self.filename_template.substitute(**{'ReaderName': reader.name}))
    
    final_df.to_csv(output_file, index=False)

    return final_df

class HistoricalDataProcessor(DataProcessor):
  def __init__(self):
    self.output_dir_template = Template("$DataBaseDir/$ProcessedDataDir/$HistoricalDataDir/")
    self.monthly_group_data_filename = Template("$ReaderName-$MonthlySuffix.csv")
    self.annual_group_data_filename = Template("$ReaderName-$AnnualSuffix.csv")

  def process(self, reader: DataReader, options: dict):
    from_date = MarketDaysHelper.get_this_or_next_market_day(options['from_date'])
    to_date = MarketDaysHelper.get_this_or_previous_market_day(options['to_date'])

    historical_data = self.get_data(reader, options, from_date, to_date)
    manual_data = self.get_manual_data(reader, options, from_date, to_date)

    self.rename_columns(reader, historical_data)
    
    if manual_data is not None:
      historical_data = pd.concat([historical_data, manual_data], ignore_index=True).reset_index(drop=True).sort_values('Date')

    processed_data = self.remove_unnamed_columns(pd.DataFrame(historical_data).reset_index(drop=True))
    identifier_grouped = self.add_basic_calc(processed_data)
    yearly_grouped = self.add_yearly_growth_calc(processed_data)
    monthly_grouped = self.add_monthly_growth_calc(processed_data)

    output_path = self.output_dir_template.substitute(**EnvironmentSettings.Paths)
    monthly_data_filename = self.monthly_group_data_filename.substitute({**EnvironmentSettings.Paths, 'ReaderName': reader.name})
    annual_data_filename = self.annual_group_data_filename.substitute({**EnvironmentSettings.Paths, 'ReaderName': reader.name})

    monthly_grouped.last().reset_index().to_csv(output_path + monthly_data_filename)
    yearly_grouped.last().reset_index().to_csv(output_path + annual_data_filename)
    
    return {
      'processed_data': processed_data, 
      'yearly_grouped': yearly_grouped, 
      'monthly_grouped': monthly_grouped,
      'identifier_grouped': identifier_grouped
    }
  
  @Instrumentation.trace(name="add_basic_calc")
  def add_basic_calc(self, processed_data):
    Instrumentation.debug('Started basic calculation')

    identifier_grouped = processed_data.groupby('Identifier')
    processed_data['High'] = pd.to_numeric(processed_data['High'].replace("-", None))
    processed_data['Low'] = pd.to_numeric(processed_data['Low'].replace("-", None))

    processed_data['High'].fillna(processed_data['Close'], inplace=True)
    processed_data['Low'].fillna(processed_data['Close'], inplace=True)
 
    processed_data['Growth'] = identifier_grouped['Close'].transform(lambda x:
                                (x - x.iloc[0]) / x.iloc[0] * 100      
                              )
    
    processed_data['Historic High'] = identifier_grouped['High'].transform(lambda x:
                              x.expanding().max()    
                            )
    #for identifier in processed_data['Identifier'].unique():
    #  processed_data.loc[processed_data['Identifier'] == identifier, 'ATH'] = processed_data[processed_data['Identifier'] == identifier]['High'].expanding().max()

    processed_data['Low From Historic High'] = (processed_data['Close'] - processed_data['Historic High']) / processed_data['Historic High'] * 100
    processed_data['Low From Historic High'] = 0

    return identifier_grouped

  def add_periodic_growth_calc(self, processed_data, period, label):
    Instrumentation.debug(f"Started periodic calculation for {period}")

    periodic_grouped = processed_data.groupby(['Identifier', period])

    processed_data[period + ' Open'] = periodic_grouped['Open'].transform(lambda x: x.iloc[0])
    processed_data[period + ' Close'] = periodic_grouped['Close'].transform(lambda x: x.iloc[-1])
    processed_data[period + ' Low'] = periodic_grouped['Low'].transform('min')
    processed_data[period + ' High'] = periodic_grouped['High'].transform('max')
    processed_data[label + ' - Growth'] = periodic_grouped['Close'].transform(lambda x: 
                                          #pd.Series((x.iloc[-1] - x.iloc[0]) / x.iloc[0] * 100 if x.iloc[0] > 0 else 0, dtype='float64')
                                          (x.iloc[-1] - x.iloc[0]) / x.iloc[0] * 100
                                        )
    processed_data[label + ' - Highest Fall From Start'] = periodic_grouped['Low'].transform(lambda x: 
                                          #pd.Series((x.min() - x.iloc[0]) / x.iloc[0] * 100 if x.iloc[0] > 0 else 0, dtype='float64')
                                          (x.min() - x.iloc[0]) / x.iloc[0] * 100
                                        )
    processed_data[label + ' - Highest Fall From Historic High'] = periodic_grouped['Low From Historic High'].transform('min')
    processed_data[label + ' - Highest Rise From Start'] = periodic_grouped['High'].transform(lambda x: 
                                          #pd.Series((x.max() - x.iloc[0]) / x.iloc[0] * 100 if x.iloc[0] > 0 else 0, dtype='float64')
                                          (x.max() - x.iloc[0]) / x.iloc[0] * 100
                                        )
    processed_data[label + ' - Turnover'] = periodic_grouped['Turnover (Rs. Cr.)'].transform('sum')

    return periodic_grouped

  @Instrumentation.trace(name="add_yearly_growth_calc")
  def add_yearly_growth_calc(self, processed_data):
    processed_data['Year'] = processed_data['Date'].dt.year
    return self.add_periodic_growth_calc(processed_data, 'Year', 'Annual')
  
  @Instrumentation.trace(name="add_monthly_growth_calc")
  def add_monthly_growth_calc(self, processed_data):
    processed_data['Month'] = processed_data['Date'].dt.strftime("%Y-%m")
    return self.add_periodic_growth_calc(processed_data, 'Month', 'Monthly')

  def rename_columns(self, reader: DataReader, historical_data: pd.DataFrame):
    column_name_mappings = reader.get_column_name_mappings()
    if column_name_mappings is not None:
      historical_data.rename(columns = column_name_mappings, inplace = True)

  @Instrumentation.trace(name="get_manual_data")
  def get_manual_data(self, reader: DataReader, options: dict, from_date: date, to_date: date):
    manual_data_file = os.path.join(self.manual_data_dir_template.substitute(**EnvironmentSettings.Paths), 
                                self.filename_template.substitute(**{'ReaderName': reader.name}))

    if os.path.exists(manual_data_file):
      manual_data = pd.read_csv(manual_data_file)
      manual_data['Date'] = pd.to_datetime(manual_data['Date'])
      return manual_data[manual_data['Date'].dt.date.between(from_date, to_date)]
    else:
      return None

  @Instrumentation.trace(name="get_data")
  def get_data(self, reader: DataReader, options: dict, from_date: date, to_date: date):
    Instrumentation.debug("Started to read data")
    output_file = os.path.join(self.output_dir_template.substitute(**EnvironmentSettings.Paths), 
                               self.filename_template.substitute(**{'ReaderName': reader.name}))
    
    if reader.options.cutoff_date is not None and from_date < reader.options.cutoff_date:
      Instrumentation.debug(f"Reading from {reader.options.cutoff_date} instead of {from_date}")
      from_date = reader.options.cutoff_date
    
    dateRangeReader = DateRangeDataReader(reader)
    
    save_to_file = False

    if os.path.exists(output_file):
      historical_data = pd.read_csv(output_file)
      earliest = pd.Timestamp(historical_data['Date'].min()).date()
      latest = pd.Timestamp(historical_data['Date'].max()).date()
      if earliest > from_date:
        Instrumentation.info(f"Reading data from {from_date} to {earliest}")
        read_data = dateRangeReader.read(from_date, earliest)
        historical_data = pd.concat([historical_data, read_data], ignore_index=True).reset_index(drop=True)
        save_to_file = True
      
      if latest < to_date:
        Instrumentation.info(f"Reading data from {latest} to {to_date}")
        read_data = dateRangeReader.read(latest, to_date)
        historical_data = pd.concat([historical_data, read_data], ignore_index=True).reset_index(drop=True)
        save_to_file = True
    else:
      Instrumentation.info(f"Reading data from {from_date} to {to_date}")
      historical_data = dateRangeReader.read(from_date, to_date)
      save_to_file = True
      
    historical_data['Date'] = pd.to_datetime(historical_data['Date'])
    
    if save_to_file == True:
      Instrumentation.debug(f"Saving data to file: {output_file}")
      historical_data = self.remove_unnamed_columns(historical_data.sort_values('Date'))
      historical_data.to_csv(output_file, index=False)
    
    return historical_data[historical_data['Date'].dt.date.between(from_date, to_date)]