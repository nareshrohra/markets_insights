from string import Template

class AggregationPeriods:
  Monthly = "Monthly"
  Annual = "Annual"

class BasePriceColumns:
  Open = 'Open'
  High = 'High'
  Low = 'Low'
  Close = 'Close'

class BaseColumns(BasePriceColumns):
  Identifier = 'Identifier'
  PreviousClose = 'PreviousClose'
  Date = 'Date'
  Turnover: str = 'Turnover'
  Volume: str = 'Volume'

PeriodAggregateColumnTemplate: Template = Template("$period $col_name")

class CalculatedColumnsBase:
  Month = 'Month'
  MonthNo = 'MonthNo'
  Year = 'Year'
  Day = 'Day'
  Vwap = 'Vwap'
  PriceCrossedAboveVwap = 'PriceCrossedAboveVwap'

class CalculatedColumns (CalculatedColumnsBase):
  Change = 'Change'
  IsInDerivatives = 'IsInDerivatives'
  ClosePriceDiff = 'ClosePriceDiff'
  Gain = 'Gain'
  Loss = 'Loss'
  AvgGain = 'AvgGain'
  AvgLoss = 'AvgLoss'
  RelativeStrength = 'Rs'
  RelativeStrengthIndex = 'Rsi'
  RsiCrossedAbove = 'RsiCrossedAbove'
  RsiCrossedBelow = 'RsiCrossedBelow'
  StochRsi_K = 'StochRsi_K'
  StochRsi_D = 'StochRsi_D'
  StochRsi_KCrossedAbove = 'StochRsi_KCrossedAbove'
  StochRsi_KCrossedBelow = 'StochRsi_KCrossedBelow'
  StochRsi_DCrossedAbove = 'StochRsi_DCrossedAbove'
  StochRsi_DCrossedBelow = 'StochRsi_DCrossedBelow'

class DerivativesCalculatedColumns (CalculatedColumnsBase):
  CloseToPrevCloseChangePerc: str = 'CloseToPrevCloseChangePerc'
  OpenToPrevCloseChangePerc: str = 'OpenToPrevCloseChangePerc'
  PreviousCloseAmount: str = 'PreviousCloseAmount'
  OpenAmount: str = 'OpenAmount'
  CloseAmount: str = 'CloseAmount'
  HighAmount: str = 'HighAmount'
  LowAmount: str = 'LowAmount'
  AmountDiffOpenToPrevClose: str = 'AmountDiffOpenToPrevClose'
  AmountDiffCloseToPrevClose: str = 'AmountDiffCloseToPrevClose'
  AmountDiffOpenToPrevClose: str = 'AmountDiffOpenToPrevClose'
  ClosestStrike: str = 'ClosestStrike'
  LotSize: str = 'LotSize'

class DerivativesBaseColumns(BaseColumns):
  InstrumentType: str = 'InstrumentType'
  AveragePrice: str = 'AvrgPric'
  OptionType: str = 'OptionType'
  ExpiryDate: str = 'ExpiryDate'
  StrikePrice: str = 'StrkPric'
  OpenInterest: str = 'OpenInterest'
  OiChangePct: float = 'OiChangePerc'
