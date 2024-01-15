from market_insights.core.environment import EnvironmentSettings
import os
from string import Template
import pandas as pd
from market_insights.datareader.data_reader import DataReader, DateRangeDataReader
from market_insights.core.column_definition import BaseColumns, CalculatedColumns
from market_insights.core.core import MarketDaysHelper, Instrumentation
from market_insights.core.column_definition import BaseColumns
from market_insights.calculations.equity import LowestPriceInNextNDaysCalculationWorker, HighestPriceInNextNDaysCalculationWorker
from market_insights.calculations.base import CalculationWorker, CalculationPipeline, \
  SmaCalculationWorker, RsiCalculationWorker, BollingerBandCalculationWorker, StochRsiCalculationWorker, \
  ValueCrossedAboveFlagWorker, ValueCrossedBelowFlagWorker, PriceCrossedAboveValueFlagWorker, PriceCrossedBelowValueFlagWorker, \
  StdDevCalculationWorker

from datetime import date
import glob
from typing import Dict

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

class CalculationPipelineBuilder:

  def create_pipeline_for_worker(worker: CalculationWorker):
    pipeline = CalculationPipeline()
    pipeline.add_calculation_worker(worker)
    return pipeline
  
  def create_pipeline_for_workers(workers: list[CalculationWorker]):
    pipeline = CalculationPipeline()
    for worker in workers:
      pipeline.add_calculation_worker(worker)
    return pipeline
  
  def create_bb_calculation_pipeline(windows = [200], deviations = [2, 3]):
    pipeline = CalculationPipeline()
    for window in windows:
      pipeline.add_calculation_worker(StdDevCalculationWorker(time_window=window))
      for deviation in deviations:
        worker = BollingerBandCalculationWorker(window, deviation)
        pipeline.add_calculation_worker(worker)
        pipeline.add_calculation_worker(PriceCrossedBelowValueFlagWorker(worker._column_name_lower_band))
        pipeline.add_calculation_worker(PriceCrossedAboveValueFlagWorker(worker._column_name_upper_band))
    return pipeline
  
  def create_sma_calculation_pipeline(windows = [50, 100, 200]):
    pipeline = CalculationPipeline()
    for window in windows:
      worker = SmaCalculationWorker(window)
      pipeline.add_calculation_worker(worker)
      pipeline.add_calculation_worker(PriceCrossedBelowValueFlagWorker(worker._column_name))
      pipeline.add_calculation_worker(PriceCrossedAboveValueFlagWorker(worker._column_name))
    return pipeline
  
  def create_rsi_calculation_pipeline(crossing_above_flag_value = 75, crossing_below_flag_value = 30, window = 14):
    pipeline = CalculationPipeline()
    pipeline.add_calculation_worker(RsiCalculationWorker(window))
    if crossing_above_flag_value is not None:
      pipeline.add_calculation_worker(ValueCrossedAboveFlagWorker(CalculatedColumns.RelativeStrengthIndex, crossing_above_flag_value))
    if crossing_below_flag_value is not None:
      pipeline.add_calculation_worker(ValueCrossedBelowFlagWorker(CalculatedColumns.RelativeStrengthIndex, crossing_below_flag_value))
    return pipeline
  
  def create_stoch_rsi_calculation_pipeline(crossing_above_flag_value = 80, crossing_below_flag_value = 20, window = 14):
    pipeline = CalculationPipeline()
    pipeline.add_calculation_worker(StochRsiCalculationWorker(window))
    if crossing_above_flag_value is not None:
      pipeline.add_calculation_worker(ValueCrossedAboveFlagWorker(CalculatedColumns.StochRsi_K, crossing_above_flag_value))
      pipeline.add_calculation_worker(ValueCrossedAboveFlagWorker(CalculatedColumns.StochRsi_D, crossing_above_flag_value))
    if crossing_below_flag_value is not None:
      pipeline.add_calculation_worker(ValueCrossedBelowFlagWorker(CalculatedColumns.StochRsi_K, crossing_below_flag_value))
      pipeline.add_calculation_worker(ValueCrossedBelowFlagWorker(CalculatedColumns.StochRsi_D, crossing_below_flag_value))
    return pipeline
  
  def create_forward_looking_price_fall_pipeline(n_days_list):
    forward_looking_lowest_price_pipeline = CalculationPipeline()
    for n in n_days_list:
      forward_looking_lowest_price_pipeline.add_calculation_worker(LowestPriceInNextNDaysCalculationWorker(n))
    return forward_looking_lowest_price_pipeline
  
  def create_forward_looking_price_rise_pipeline(n_days_list):
    forward_looking_highest_price_pipeline = CalculationPipeline()
    for n in n_days_list:
      forward_looking_highest_price_pipeline.add_calculation_worker(HighestPriceInNextNDaysCalculationWorker(n))
    return forward_looking_highest_price_pipeline

class MultiDataCalculationPipelines:
  def __init__(self):
    self._store: Dict[str, CalculationPipeline] = {}

  def set_item(self, k: str, v: CalculationPipeline) -> None:
    self._store[k] = v
    
  def get_item(self, k: str) -> CalculationPipeline:
    return self._store[k]
  
  def run(self, data):
    for key in self._store:
      result = self._store[key].run(data)
      if result is not None:
        data = result
    return data

class HistoricalDataset():
  _daily : pd.DataFrame
  _identifier_grouped : pd.DataFrame
  _monthly : pd.DataFrame
  _annual : pd.DataFrame
  
  def create_identifier_grouped(self):
    self._identifier_grouped = self.create_grouped_data(BaseColumns.Identifier)
    return self
  
  def set_daily_data(self, daily):
    self._daily = daily
    return self
  
  def set_monthly_data(self, monthly):
    self._monthly = monthly
    return self

  def set_annual_data(self, annual):
    self._annual = annual
    return self
  
  def sanitize_data(self):
    self._daily['High'] = pd.to_numeric(self._daily['High'].replace("-", None))
    self._daily['Low'] = pd.to_numeric(self._daily['Low'].replace("-", None))

    self._daily['High'].fillna(self._daily['Close'], inplace=True)
    self._daily['Low'].fillna(self._daily['Close'], inplace=True)
    return self

  def create_grouped_data(self, columns):
    return self._daily.groupby(columns)
  
  def get_identifier_grouped(self):
    return self._identifier_grouped
  
  def get_daily_data(self):
    return self._daily
  
  def get_monthly_data(self):
    return self._monthly
  
  def get_annual_data(self):
    return self._annual

  def process(self):
    self.sanitize_data()
    self.create_grouped_data()
    return self

class HistoricalDataProcessOptions:
  include_monthly_data: bool = True
  include_annual_data: bool = True

class HistoricalDataProcessor(DataProcessor):
  options: HistoricalDataProcessOptions

  def __init__(self, options: HistoricalDataProcessOptions = HistoricalDataProcessOptions()):
    self.output_dir_template = Template("$DataBaseDir/$ProcessedDataDir/$HistoricalDataDir/")
    self.monthly_group_data_filename = Template("$ReaderName-$MonthlySuffix.csv")
    self.annual_group_data_filename = Template("$ReaderName-$AnnualSuffix.csv")
    self.historic_highs_reset_days = 60
    self.dataset: HistoricalDataset
    self.calculation_pipelines: MultiDataCalculationPipelines
    self.options = options
    
  def set_calculation_pipelines(self, pipelines):
    self.calculation_pipelines = pipelines

  def run_calculation_pipelines(self):
    daily_data = self.calculation_pipelines.run(self.dataset.get_daily_data())
    if daily_data is not None:
      self.dataset.set_daily_data(daily_data)

  @Instrumentation.trace(name="process")
  def process(self, reader: DataReader, options: dict):
    from_date = MarketDaysHelper.get_this_or_next_market_day(options['from_date'])
    to_date = MarketDaysHelper.get_this_or_previous_market_day(options['to_date'])

    historical_data = self.get_data(reader, options, from_date, to_date)
    manual_data = self.get_manual_data(reader, options, from_date, to_date)

    #self.rename_columns(reader, historical_data)
    
    if manual_data is not None:
      historical_data = pd.concat([historical_data, manual_data], ignore_index=True).reset_index(drop=True).sort_values('Date')

    historical_data[BaseColumns.Identifier] = historical_data[BaseColumns.Identifier].str.upper()

    self.dataset = HistoricalDataset()
    daily_data = self.remove_unnamed_columns(pd.DataFrame(historical_data).reset_index(drop=True))
    self.dataset.set_daily_data(daily_data)
    self.dataset.create_identifier_grouped()
    self.add_basic_calc(daily_data)

    output_path = self.output_dir_template.substitute(**EnvironmentSettings.Paths)
    
    if self.options.include_monthly_data == True:
      self.dataset.set_monthly_data(self.add_monthly_growth_calc(daily_data))
      monthly_data_filename = self.monthly_group_data_filename.substitute({**EnvironmentSettings.Paths, 'ReaderName': reader.name})
      self.dataset.get_monthly_data().last().reset_index().to_csv(output_path + monthly_data_filename)
    
    if self.options.include_annual_data == True:
      self.dataset.set_annual_data(self.add_yearly_growth_calc(daily_data))
      annual_data_filename = self.annual_group_data_filename.substitute({**EnvironmentSettings.Paths, 'ReaderName': reader.name})
      self.dataset.get_annual_data().last().reset_index().to_csv(output_path + annual_data_filename)
    
    return self.dataset
  
  @Instrumentation.trace(name="add_basic_calc")
  def add_basic_calc(self, processed_data):
    Instrumentation.debug('Started basic calculation')

    processed_data['Growth'] = self.dataset.get_identifier_grouped()['Close'].transform(lambda x:
        (x - x.iloc[0]) / x.iloc[0] * 100      
      )
    
    # processed_data['Historic High'] = self.dataset.get_identifier_grouped()['High'].transform(lambda x:
    #     x.expanding().max()    
    #   )
    
    # processed_data['ATH'] = identifier_grouped['Date'].transform( lambda x :
    #                           identifier_grouped['High'][identifier_grouped['Date'] <= (x + timedelta(days=self.historic_highs_reset_days))].max()
    #                         )

    #for identifier in processed_data['Identifier'].unique():
    #  processed_data.loc[processed_data['Identifier'] == identifier, 'ATH'] = processed_data[processed_data['Identifier'] == identifier]['High'].expanding().max()

    #processed_data['Low From Historic High'] = (processed_data['Close'] - processed_data['Historic High']) / processed_data['Historic High'] * 100
    return processed_data

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