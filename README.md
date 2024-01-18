
# Markets Insights 📝  
This package fetches and processes capital markets data from NSE (National Stock Exchange, India). Following data can be retrieved
1. Index (Nifty, Bank Nifty, NiftyIT)
2. Stocks
3. Derivatives (Futures and Options)

*Support for additional markets and instruments can be added externally*

The package can perform technical functions on price of Index and Stocks. Following indicators are supported.

1. Simple Moving Averages (SMA)
2. Relative Strength Index (RSI)
3. Stochastic RSI
4. Bollinger Bands (with standard deviations)

The calculation pipeline is quite extensible and more idicators can be added externally.

## Getting Started 🚀
### Installation
```python
!pip install markets_insights
import markets_insights
```

### Get Index data for date range
```python
from markets_insights.datareader import data_reader
import datetime

reader = data_reader.NseIndicesReader()

daterange_reader = data_reader.DateRangeDataReader(reader)

from_date = datetime.date(2023, 1, 1)
to_date = datetime.date.today() + datetime.timedelta(days=-1)
result = daterange_reader.read(from_date = from_date, to_date = to_date)
```

### Calculation pipeline for RSI
Below example demonstrates calculating RSI using the calculation pipeline. The datepart calculation is pre-requisite for RSI calculation.

```python
# import classes & setup options
import datetime
from markets_insights.datareader.data_reader import BhavCopyReader
from markets_insights.dataprocess.data_processor import HistoricalDataProcessor, MultiDataCalculationPipelines, CalculationPipelineBuilder, HistoricalDataProcessOptions
from markets_insights.calculations.base import DatePartsCalculationWorker

reader = BhavCopyReader()
options = HistoricalDataProcessOptions()
options.include_monthly_data = False
options.include_annual_data = False
histDataProcessor = HistoricalDataProcessor(options)

# Fetch the data
year_start = datetime.date(2023, 1, 1)
to_date = datetime.date(2023, 12, 31)
result = histDataProcessor.process(reader, {'from_date': year_start, 'to_date': to_date})

# Prepare calculation pipeline
pipelines = MultiDataCalculationPipelines()
pipelines.set_item('date_parts', CalculationPipelineBuilder.create_pipeline_for_worker(DatePartsCalculationWorker()))
pipelines.set_item('rsi', CalculationPipelineBuilder.create_rsi_calculation_pipeline())
histDataProcessor.set_calculation_pipelines(pipelines)

# Run the pipeline
histDataProcessor.run_calculation_pipelines()
```

### A real use case: Understand the affect of RSI on price
In this use case, we understand the affect of RSI on the price of equity/stock.

#### Preparing the data
We perform below steps to prepare our analysis data
- Calculate RSI for each day for all the stocks.
- Add a flag for whenever the RSI crosses the control limits (eg: above 75 and below 30)
- Calculate the highest and lowest price change in the next 1, 3, 5, 7 & 10 trading sessions.
- Find the median for highest price change and lowest price change whenever the RSI crosses the control limits.


```python
# prepare calculation pipeline
periods = [1, 3, 5, 7, 10]

pipelines = MultiDataCalculationPipelines()
pipelines.set_item('date_parts', CalculationPipelineBuilder.create_pipeline_for_worker(DatePartsCalculationWorker()))
pipelines.set_item('rsi', CalculationPipelineBuilder.create_rsi_calculation_pipeline(crossing_above_flag_value = 75, crossing_below_flag_value = 30, window = 14))
pipelines.set_item('foward_looking_fall', CalculationPipelineBuilder.create_forward_looking_price_fall_pipeline(periods))
pipelines.set_item('foward_looking_rise', CalculationPipelineBuilder.create_forward_looking_price_rise_pipeline(periods))
histDataProcessor.set_calculation_pipelines(pipelines=pipelines)

# run the pipeline and show results
histDataProcessor.run_calculation_pipelines()

daily_data = result.get_daily_data()

# Import constants so its easier to refer to column names
from markets_insights.core.column_definition import CalculatedColumns

# get names of fwd looking price column names. Since, these column names are auto-generated there no constants for them
fwd_looking_price_fall_cols, fwd_looking_price_rise_cols = [x for x in daily_data.columns if 'HighestPercFallInNext' in x], \
  [x for x in daily_data.columns if 'HighestPercRiseInNext' in x]
```

#### Show the median price change % for highest price fall whenever the RSI crosses above
```python
daily_data[
(daily_data[CalculatedColumns.RsiCrossedAbove])
][fwd_looking_price_fall_cols].median()
```
*Output*
```bat
HighestPercFallInNext1Days     3.245288
HighestPercFallInNext3Days     4.623437
HighestPercFallInNext5Days     5.228839
HighestPercFallInNext7Days     5.719615
HighestPercFallInNext10Days    6.158358
dtype: float64
```

#### Show the median price change % for highest price rise whenever the RSI crosses below
```python
daily_data[
(daily_data[CalculatedColumns.RsiCrossedAbove])
][fwd_looking_price_rise_cols].median()
```
*Output*
```bat
HighestPercRiseInNext1Days     0.985232
HighestPercRiseInNext3Days     1.550388
HighestPercRiseInNext5Days     2.071982
HighestPercRiseInNext7Days     2.640740
HighestPercRiseInNext10Days    3.314917
dtype: float64
```

### Creating a DataReader
In this example we will create a new data reader to read data for Nasdaq listed equities. We will use **yfinance** python library for this.

#### Import classes
```python
from markets_insights.datareader.data_reader import DateRangeDataReader
from markets_insights.core.core import Instrumentation
from markets_insights.core.column_definition import BaseColumns

import yfinance as yf
import pandas
```

#### Create reader class
We will create a class that extends the base reader. yfinance library can read data for a range. So, we will extend *DateRangeDataReader* class. With yfinance library, we have to specify which equity/tickers we want to download. For the sake of this example, we will download for top 7 companies of Nasdaq.

```python
class NasdaqDataReader (DateRangeDataReader):
  def __init__(self, tickers: list = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'META', 'TSLA', 'NVDA']):
    super().__init__(reader=None)
    self.tickers = tickers
    self.name = "NasdaqDataReader"

  @Instrumentation.trace(name="NasdaqDataReader.read")
  def read(self, from_date, to_date):
    df_list = list()
    for ticker in self.tickers:
        data = yf.download(ticker, group_by="Ticker", start=from_date, end=to_date)
        data['ticker'] = ticker
        df_list.append(data)

    # combine all dataframes into a single dataframe
    df = pandas.concat(df_list)

    data = df.reset_index().rename(columns = self.get_column_name_mappings())
    data[BaseColumns.Date] = pandas.to_datetime(data[BaseColumns.Date])
    return data
  
  def get_column_name_mappings(self):
    return {
      'ticker': BaseColumns.Identifier,
      'OPEN': BaseColumns.Open,
      'HIGH': BaseColumns.High,
      'LOW': BaseColumns.Low,
      'CLOSE': BaseColumns.Close
    }
```

*Notice here we are renaming the columns to standard column names so that the calculation pipeline can read them properly.*

#### Running the calculation pipeline
The calculation pipeline will not be different except we will pass NasdaqDataReader instance.

```python
# import classes & setup options
from markets_insights.dataprocess.data_processor import HistoricalDataProcessor, MultiDataCalculationPipelines, CalculationPipelineBuilder, HistoricalDataProcessOptions
from markets_insights.calculations.base import DatePartsCalculationWorker

reader = NasdaqDataReader()
options = HistoricalDataProcessOptions()
options.include_monthly_data = False
options.include_annual_data = False
histDataProcessor = HistoricalDataProcessor(options)

# Fetch the data
year_start = datetime.date(2023, 1, 1)
to_date = datetime.date(2023, 12, 31)
result = histDataProcessor.process(reader, {'from_date': year_start, 'to_date': to_date})

# Prepare calculation pipeline
pipelines = MultiDataCalculationPipelines()
pipelines.set_item('date_parts', CalculationPipelineBuilder.create_pipeline_for_worker(DatePartsCalculationWorker()))
pipelines.set_item('rsi', CalculationPipelineBuilder.create_rsi_calculation_pipeline())
histDataProcessor.set_calculation_pipelines(pipelines)

# Run the pipeline
histDataProcessor.run_calculation_pipelines()
```

Here's the code to display results
```python
from markets_insights.core.column_definition import CalculatedColumns

result.get_daily_data() \
  .sort_values(
    [BaseColumns.Date, BaseColumns.Identifier]
  )[
    [BaseColumns.Identifier, BaseColumns.Date, BaseColumns.Close, 
     CalculatedColumns.RelativeStrengthIndex]
  ] \
  .tail(5)
```

*Output*

|      | Identifier   | Date                |   Close |     Rsi |
|-----:|:-------------|:--------------------|--------:|--------:|
|  248 | AAPL         | 2023-12-28 00:00:00 |  193.58 | 54.4815 |
|  497 | AMZN         | 2023-12-28 00:00:00 |  153.38 | 63.9387 |
|  746 | GOOGL        | 2023-12-28 00:00:00 |  140.23 | 61.585  |
|  995 | META         | 2023-12-28 00:00:00 |  358.32 | 70.2377 |
| 1244 | MSFT         | 2023-12-28 00:00:00 |  375.28 | 56.909  |
| 1493 | NVDA         | 2023-12-28 00:00:00 |  495.22 | 58.305  |
| 1742 | TSLA         | 2023-12-28 00:00:00 |  253.18 | 55.9788 |

### Creating a CalculationWorker
In this example, we will create a CalculationWorker to calcualte the Fibonacci Retracement level for any equity or index. Finbonacci Retracement levels are based on a time window and a level (26.3%, 50% etc). So, these will become input to our CalculationWorker. Lets call this worker as **FibnocciRetracementCalculationWorker**

#### Implement the worker class. The important aspect here is to override the `add_calculated_columns()` method
```python
## import modules
from markets_insights.calculations.base import CalculationWorker
from markets_insights.core.core import Instrumentation
from markets_insights.calculations.base import BaseColumns
import pandas

class FibnocciRetracementCalculationWorker (CalculationWorker):
  def __init__(self, time_window: int, level_perct: float):
    self._time_window = time_window
    self._level = level_perct / 100
    self._column_name = 'Fbr' + str(level_perct)

  @Instrumentation.trace(name="FibnocciRetracementCalculationWorker")
  def add_calculated_columns(self, data: pandas.DataFrame):
    identifier_grouped_data: pandas.DataFrame = data.groupby(BaseColumns.Identifier)
    #Since, our dataframe may contain data for multiple symbols, we need to first group them by Identifier
    data[self._column_name] = identifier_grouped_data[BaseColumns.Close].transform(
        lambda x: 
          x.rolling(self._time_window).max() - 
          (
            (x.rolling(self._time_window).max() - x.rolling(self._time_window).min())  * self._level
          )
      )
```
#### Create pipline with the FibnocciRetracementCalculationWorker and run
Now, that our worker is created let us use it in a calculation pipeline. We can use it with any instrument (index, stock) that are supported. Event for the Nasdaq instruments that were supported in earlier examples. For this example, let us take NSE Indexes.
```python
from markets_insights.datareader.data_reader import NseIndicesReader
from markets_insights.dataprocess.data_processor import HistoricalDataProcessor, HistoricalDataProcessOptions, \
  MultiDataCalculationPipelines, CalculationPipeline
histDataProcessor = HistoricalDataProcessor(HistoricalDataProcessOptions(include_monthly_data=False, include_annual_data=False))

# Fetch the data
result = histDataProcessor.process(NseIndicesReader(), {'from_date': datetime.date(2023, 12, 1), 'to_date': datetime.date(2023, 12, 31)})

# Prepare calculation pipeline
fbr50_worker = FibnocciRetracementCalculationWorker(time_window=7, level_perct=50)
pipelines = MultiDataCalculationPipelines()
histDataProcessor.set_calculation_pipelines(
  CalculationPipeline(
    workers = [fbr50_worker]
  )
)

# Run the pipeline and get data
histDataProcessor.run_calculation_pipelines()
```

##### Display the results. 
Since our time window was 15 days. So, the calculation result for first 14 days will not be available. We will look at the last 10 records with `tail(10)`
```python
result.get_daily_data()[[
  BaseColumns.Identifier, BaseColumns.Date, BaseColumns.Close, fbr50_worker._column_name
]].tail(10)
```

*Output*
|      | Identifier                        | Date                |    Close |    Fbr50 |
|-----:|:----------------------------------|:--------------------|---------:|---------:|
| 2136 | NIFTY500 SHARIAH                  | 2023-12-29 00:00:00 |  6410.93 |  6282.76 |
| 2137 | NIFTY CPSE                        | 2023-12-29 00:00:00 |  4860.45 |  4755.07 |
| 2138 | NIFTY100 LIQUID 15                | 2023-12-29 00:00:00 |  5827    |  5756.95 |
| 2139 | NIFTY100 EQUAL WEIGHT             | 2023-12-29 00:00:00 | 26879.7  | 26315.2  |
| 2140 | NIFTY HIGH BETA 50                | 2023-12-29 00:00:00 |  3401.85 |  3315.58 |
| 2141 | NIFTY ALPHA 50                    | 2023-12-29 00:00:00 | 42306.3  | 41655.7  |
| 2142 | NIFTY GROWTH SECTORS 15           | 2023-12-29 00:00:00 | 10787.7  | 10646.7  |
| 2143 | NIFTY MIDSMALLCAP 400             | 2023-12-29 00:00:00 | 16015.5  | 15663    |
| 2144 | NIFTY 50                          | 2023-12-29 00:00:00 | 21731.4  | 21464.4  |
| 2145 | NIFTY 15 YR AND ABOVE G-SEC INDEX | 2023-12-29 00:00:00 |  3004.56 |  3004.09 |
