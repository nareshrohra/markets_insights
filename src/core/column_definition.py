class BaseColumns:
  Identifier = "Identifier"
  PreviousClose = "PreviousClose"
  Open = "Open"
  High = "High"
  Low = "Low"
  Close = "Close"
  Date = "Date"

class CalculatedColumns:
    ClosePriceDiff = "ClosePriceDiff"
    Gain = "Gain"
    Loss = "Loss"
    AvgGain = "AvgGain"
    AvgLoss = "AvgLoss"
    RelativeStrength = "Rs"
    RelativeStrengthIndex = "Rsi"
    RsiCrossedAbove = "RsiCrossedAbove"
    RsiCrossedBelow = "RsiCrossedBelow"

class DerivativesCalculatedColumns:
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
  AveragePrice: str = 'AvrgPric'
  OptionType: str = 'OptionType'
  ExpiryDate: str = 'ExpiryDate'
  StrikePrice: str = 'StrkPric'
  Turnover: str = 'Turnover (Rs. Cr.)'
  OpenInterest: str = 'OpenInterest'
