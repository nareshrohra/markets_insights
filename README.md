
# Markets Insights 📝  
This package fetches and processes capital markets data from NSE (National Stock Exchange, India). Following data can be retrieved
1. Index (Nifty, Bank Nifty, NiftyIT)
2. Stocks
3. Derivatives (Futures and Options)

The package can perform technical functions on price of Index and Stocks. Following functions are supported.

1. Simple Moving Averages (SMA)
2. Relative Strength Index (RSI)
3. Stochastic RSI
4. Bollinger Bands (with standard deviations)

The calculation pipeline is quite extensible and more functions can be added externally.

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