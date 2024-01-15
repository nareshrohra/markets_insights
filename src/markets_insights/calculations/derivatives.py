from markets_insights.core.column_definition import DerivativesBaseColumns, DerivativesCalculatedColumns
from markets_insights.core.core import Instrumentation
from markets_insights.dataprocess.data_processor import CalculationWorker
import pandas as pd

class DerivativesPriceCalculationWorker (CalculationWorker):
  @Instrumentation.trace(name="DerivativesPriceCalculationWorker")
  def add_calculated_columns(self, data):
    if DerivativesBaseColumns.PreviousClose not in data.columns.to_list():
      data[DerivativesBaseColumns.PreviousClose] = data.groupby([DerivativesBaseColumns.Identifier, DerivativesBaseColumns.OptionType, DerivativesBaseColumns.ExpiryDate, DerivativesBaseColumns.StrikePrice])[DerivativesBaseColumns.Close].transform(lambda x: x.shift(1))

    data[DerivativesCalculatedColumns.CloseToPrevCloseChangePerc] = (data[DerivativesBaseColumns.Close] / data[DerivativesBaseColumns.PreviousClose] - 1) * 100
    data[DerivativesCalculatedColumns.OpenToPrevCloseChangePerc] = (data[DerivativesBaseColumns.Open] / data[DerivativesBaseColumns.PreviousClose] - 1) * 100
    
    data[DerivativesCalculatedColumns.PreviousCloseAmount] = data[DerivativesBaseColumns.PreviousClose] * data[DerivativesCalculatedColumns.LotSize]
    data[DerivativesCalculatedColumns.OpenAmount] = data[DerivativesBaseColumns.Open] * data[DerivativesCalculatedColumns.LotSize]
    data[DerivativesCalculatedColumns.CloseAmount] = data[DerivativesBaseColumns.Close] * data[DerivativesCalculatedColumns.LotSize]
    data[DerivativesCalculatedColumns.HighAmount] = data[DerivativesBaseColumns.High] * data[DerivativesCalculatedColumns.LotSize]
    data[DerivativesCalculatedColumns.LowAmount] = data[DerivativesBaseColumns.Low] * data[DerivativesCalculatedColumns.LotSize]

    data[DerivativesCalculatedColumns.AmountDiffOpenToPrevClose] = data[DerivativesCalculatedColumns.OpenAmount] - data[DerivativesCalculatedColumns.PreviousCloseAmount]
    data[DerivativesCalculatedColumns.AmountDiffCloseToPrevClose] = data[DerivativesCalculatedColumns.CloseAmount] - data[DerivativesCalculatedColumns.PreviousCloseAmount]

class DerivativesLotSizeCalculationWorker (CalculationWorker):
  @Instrumentation.trace(name="DerivativesLotSizeCalculationWorker")
  def add_calculated_columns(self, data):
    data[DerivativesCalculatedColumns.LotSize] = data.groupby([DerivativesBaseColumns.Identifier, DerivativesBaseColumns.ExpiryDate])[DerivativesBaseColumns.OpenInterest].transform(lambda x: x.min())
  
class DerivativesLotSizeCalculationWorker_Obsolete (CalculationWorker):
  @Instrumentation.trace(name="DerivativesLotSizeCalculationWorker")
  def add_calculated_columns(self, data):
    lotsize_data = pd.read_csv("../manual_data/nse_fo_lotsize.csv")
    cur_expiry_col = lotsize_data.columns[2]
    data = pd.merge(
        data,
        lotsize_data[['SYMBOL', cur_expiry_col]],
        how="left",
        left_on=DerivativesBaseColumns.Identifier,
        right_on='SYMBOL'
    )
    data.rename(columns={cur_expiry_col: DerivativesCalculatedColumns.LotSize}, inplace=True)
    return data