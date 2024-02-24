from markets_insights.core.core import Instrumentation
from markets_insights.core.column_definition import BaseColumns, CalculatedColumns, DerivativesBaseColumns
import pandas as pd
import pandas_ta as ta

class CalculationWindow:
    def __init__(self, trailing: int = 0, leading: int = 0, ):
        self.trailing = trailing
        self.leading = leading

    def load_from_list(windows: list):
        max_leading: int = 0
        max_trailing: int = 0

        for window in windows:
            max_leading = max(max_leading, window.leading)
            max_trailing = max(max_trailing, window.trailing)
        
        return CalculationWindow(max_trailing, max_leading)

class CalculationWorker:
    def __init__(self, **params):
        self._columns: list[str] = []
        self._params: dict = params

    def get_calculation_window(self) -> CalculationWindow:
        max_leading = 0
        max_trailing = 0
        for window in [window for window in ['time_window', 'N'] if window in self._params]:
            window_val = int(self._params[window])
            max_trailing = max(max_trailing, window_val)  # Assuming negative values should set max_trailing

        return CalculationWindow(max_trailing, max_leading)

    def get_group_cols(self, columns: list[str]) -> list[str]:
        group_columns: list[str] = [DerivativesBaseColumns.Identifier]
                
        group_columns.extend(group_col for group_col in 
                [
                    DerivativesBaseColumns.OptionType,
                    DerivativesBaseColumns.ExpiryDate,
                    DerivativesBaseColumns.StrikePrice,
                    DerivativesBaseColumns.OptionType,
                ] if group_col in columns
            )

        return group_columns

    def get_columns(self) -> list[str]:
        return self._columns
    
    def get_column(self) -> str:
        return self._columns[0]
    
    def get_params(self) -> dict:
        return self._params

    def add_calculated_columns(self, data: pd.DataFrame):
        raise NotImplementedError("add_calculated_fields")


class CalculationPipeline:
    _pipeline: list[CalculationWorker]

    def __init__(self, workers: list[CalculationWorker]=None):
        if workers is not None:
            self._pipeline = workers
        else:
            self._pipeline = []

    def add_calculation_worker(self, worker: CalculationWorker):
        self._pipeline.append(worker)

    def run(self, data: pd.DataFrame):
        for worker in self._pipeline:
            result = worker.add_calculated_columns(data)
            if result is not None:
                data = result
        return data
    
    def get_calculation_window(self) -> CalculationWindow:
        return CalculationWindow.load_from_list([worker.get_calculation_window() for worker in self._pipeline])


class ColumnValueCrossedAboveFlagWorker(CalculationWorker):
    def __init__(self, value_column: str = None, value: int = 0):
        super().__init__(value_column = value_column, value = int(value))
        self._columns.append(f"{value_column}CrossedAbove")

    @Instrumentation.trace(name="ColumnValueCrossedAboveFlagWorker")
    def add_calculated_columns(self, data: pd.DataFrame):
        identifier_grouped_data = data.groupby(self.get_group_cols(data.columns))
        data[self._columns[0]] = identifier_grouped_data[self._params['value_column']].transform(
            lambda x: (x.shift(1) < self._params['value']) & (x >= self._params['value'])
        )
    
    def get_calculation_window(self) -> CalculationWindow:
        return CalculationWindow(trailing=1, leading=0)


class ColumnValueCrossedBelowFlagWorker(CalculationWorker):
    def __init__(self, value_column: str = None, value: int = 0):
        super().__init__(value_column = value_column, value = int(value))
        self._columns.append(f"{value_column}CrossedBelow")

    @Instrumentation.trace(name="ColumnValueCrossedBelowFlagWorker")
    def add_calculated_columns(self, data: pd.DataFrame):
        identifier_grouped_data = data.groupby(self.get_group_cols(data.columns))
        data[self._columns[0]] = identifier_grouped_data[self._params['value_column']].transform(
            lambda x: (x.shift(-1) > self._params['value']) & (x <= self._params['value'])
        )

    def get_calculation_window(self) -> CalculationWindow:
        return CalculationWindow(trailing=1, leading=0)
    

class PriceCrossedAboveColumnValueFlagWorker(CalculationWorker):
    def __init__(self, value_column: str = None):
        super().__init__(value_column = value_column)
        self._columns.append(f"PriceCrossedAbove{value_column}")

    @Instrumentation.trace(name="PriceCrossedAboveColumnValueFlagWorker")
    def add_calculated_columns(self, data):
        data[self._columns[0]] = (
            data[BaseColumns.Close] >= data[self._params['value_column']]
        ) & (data[BaseColumns.PreviousClose] < data[self._params['value_column']])


class PriceCrossedBelowColumnValueFlagWorker(CalculationWorker):
    def __init__(self, value_column: str = None):
        super().__init__(value_column = value_column)
        self._columns.append(f"PriceCrossedBelow{value_column}")

    @Instrumentation.trace(name="PriceCrossedBelowColumnValueFlagWorker")
    def add_calculated_columns(self, data: pd.DataFrame):
        data[self._columns[0]] = (
            data[BaseColumns.Close] < data[self._params['value_column']]
        ) & (data[BaseColumns.PreviousClose] >= data[self._params['value_column']])


class ColumnValueCrossedAboveAnotherColumnValueFlagWorker(CalculationWorker):
    def __init__(self, value_column_a: str = None, value_column_b: str = None):
        super().__init__(value_column_a=value_column_a, value_column_b=value_column_b)
        self._columns.append(f"{value_column_a}CrossedAbove{value_column_b}")

    @Instrumentation.trace(name="ColumnValueCrossedAboveAnotherColumnValueFlagWorker")
    def add_calculated_columns(self, data: pd.DataFrame):
        grouped_data = data.groupby(self.get_group_cols(data.columns))
        column_a = self._params["value_column_a"]
        column_b = self._params["value_column_b"]
        if len(data[BaseColumns.Identifier].unique()) > 1:
            data[self._columns[0]] = grouped_data.apply(
                lambda x: (x.shift(1)[column_a] < x.shift(1)[column_b])
                & (x[column_a] >= x[column_b])
            ).reset_index(level=0, drop=True)
        else:
            data[self._columns[0]] = (data.shift(1)[column_a] < data.shift(1)[column_b]) \
                & (data[column_a] >= data[column_b])
    
    def get_calculation_window(self) -> CalculationWindow:
        return CalculationWindow(trailing=1, leading=0)
    

class ColumnValueCrossedBelowAnotherColumnValueFlagWorker(CalculationWorker):
    def __init__(self, value_column_a: str = None, value_column_b: str = None):
        super().__init__(value_column_a=value_column_a, value_column_b=value_column_b)
        self._columns.append(f"{value_column_a}CrossedBelow{value_column_b}")

    @Instrumentation.trace(name="ColumnValueCrossedBelowAnotherColumnValueFlagWorker")
    def add_calculated_columns(self, data: pd.DataFrame):
        identifier_grouped_data = data.groupby(self.get_group_cols(data.columns))
        column_a = self._params["value_column_a"]
        column_b = self._params["value_column_b"]
        if len(data[BaseColumns.Identifier].unique()) > 1:
            data[self._columns[0]] = identifier_grouped_data.apply(
                lambda x: (x.shift(-1)[column_a] > x.shift(-1)[column_b])
                & (x[column_a] <= x[column_b])
            ).reset_index(level=0, drop=True)
        else:
            data[self._columns[0]] = (data.shift(-1)[column_a] > data.shift(-1)[column_b]) \
                & (data[column_a] <= data[column_b])

    def get_calculation_window(self) -> CalculationWindow:
        return CalculationWindow(trailing=1, leading=0)
    

class ColumnValueBelowFlagWorker(CalculationWorker):
    def __init__(self, value_column: str = None, value: int = 0):
        super().__init__(value_column=value_column, value=int(value))
        self._columns.append(f"{value_column}Below{str(value)}")

    @Instrumentation.trace(name="ColumnValueBelowFlagWorker")
    def add_calculated_columns(self, data: pd.DataFrame):
        data[self._columns[0]] = (
            data[self._params["value_column"]] < self._params["value"]
        )


class ColumnValueAboveFlagWorker(CalculationWorker):
    def __init__(self, value_column: str = None, value: int = 0):
        super().__init__(value_column=value_column, value=int(value))
        self._columns.append(f"{value_column}Above{str(value)}")

    @Instrumentation.trace(name="ColumnValueAboveFlagWorker")
    def add_calculated_columns(self, data: pd.DataFrame):
        data[self._columns[0]] = (
            data[self._params["value_column"]] > self._params["value"]
        )



class ColumnValueBelowAnotherColumnValueFlagWorker(CalculationWorker):
    def __init__(self, value_column_a: str = None, value_column_b: str = None):
        super().__init__(value_column_a=value_column_a, value_column_b=value_column_b)
        self._columns.append(f"{value_column_a}Below{value_column_b}")

    @Instrumentation.trace(name="ColumnValueBelowAnotherColumnValueFlagWorker")
    def add_calculated_columns(self, data: pd.DataFrame):
        data[self._columns[0]] = (
            data[self._params["value_column_a"]] < data[self._params["value_column_b"]]
        )


class ColumnValueAboveAnotherColumnValueFlagWorker(CalculationWorker):
    def __init__(self, value_column_a: str = None, value_column_b: str = None):
        super().__init__(value_column_a=value_column_a, value_column_b=value_column_b)
        self._columns.append(f"{value_column_a}Above{value_column_b}")

    @Instrumentation.trace(name="ColumnValueAboveAnotherColumnValueFlagWorker")
    def add_calculated_columns(self, data: pd.DataFrame):
        data[self._columns[0]] = (
            data[self._params["value_column_a"]] > data[self._params["value_column_b"]]
        )

class DatePartsCalculationWorker(CalculationWorker):
    def __init__(self):
        super().__init__()
        self._columns.append(CalculatedColumns.Year)
        self._columns.append(CalculatedColumns.MonthNo)
        self._columns.append(CalculatedColumns.Month)
        self._columns.append(CalculatedColumns.Day)

    @Instrumentation.trace(name="DatePartsCalculationWorker")
    def add_calculated_columns(self, data: pd.DataFrame):
        data[CalculatedColumns.Year] = data[BaseColumns.Date].dt.year
        data[CalculatedColumns.MonthNo] = data[BaseColumns.Date].dt.month
        data[CalculatedColumns.Month] = data[BaseColumns.Date].dt.strftime("%b")
        data[CalculatedColumns.Day] = data[BaseColumns.Date].dt.strftime("%A")


class SmaCalculationWorker(CalculationWorker):
    def __init__(self, time_window: int = 50):
        super().__init__(time_window = int(time_window))
        self._columns.append(f"Sma{str(time_window)}")

    @Instrumentation.trace(name="SmaCalculationWorker")
    def add_calculated_columns(self, data: pd.DataFrame):
        identifier_grouped_data = data.groupby(self.get_group_cols(data.columns))
        data[self._columns[0]] = identifier_grouped_data[BaseColumns.Close].transform(
            lambda x: x.rolling(self._params['time_window']).mean()
        )


class StdDevCalculationWorker(CalculationWorker):
    def __init__(self, time_window: int = 200):
        super().__init__(time_window = int(time_window))
        self._columns.append(f"StdDev{time_window}")

    @Instrumentation.trace(name="StdDevCalculationWorker")
    def add_calculated_columns(self, data: pd.DataFrame):
        identifier_grouped_data = data.groupby(self.get_group_cols(data.columns))
        data[self._columns[0]] = identifier_grouped_data[BaseColumns.Close].transform(
            lambda x: x.rolling(self._params['time_window']).std()
        )


class BollingerBandCalculationWorker(CalculationWorker):
    def __init__(self, time_window: int = 200, deviation: int = 2):
        super().__init__(time_window = int(time_window), deviation = int(deviation))
        self._columns.append(f"Bb{str(time_window)}Dev{str(deviation)}Lower")
        self._columns.append(f"Bb{str(time_window)}Dev{str(deviation)}Upper")

    @Instrumentation.trace(name="BollingerBandCalculationWorker")
    def add_calculated_columns(self, data: pd.DataFrame):
        sma_column = f"Sma{str(self._params['time_window'])}"
        std_dev_column = f"StdDev{str(self._params['time_window'])}"

        if sma_column in data.columns:
            SmaCalculationWorker(time_window=int(self._params['time_window'])).add_calculated_columns(data)
        
        if std_dev_column in data.columns:
            StdDevCalculationWorker(time_window=int(self._params['time_window'])).add_calculated_columns(data)

        data[self._columns[0]] = data[sma_column] - (
            data[std_dev_column] * self._params['deviation']
        )
        data[self._columns[1]] = data[sma_column] + (
            data[std_dev_column] * self._params['deviation']
        )


class RsiOldCalculationWorker(CalculationWorker):
    def __init__(self, time_window):
        super().__init__()
        self._time_window = time_window
        self._column_name = "Rsi" + str(self._time_window)

    def calculate_wsm_average(self, raw_data, data, avg_col_name, abs_col_name):
        step = 1
        for i, row in enumerate(data[avg_col_name].iloc[self._time_window + step :]):
            raw_data.at[data.index[i + self._time_window + step], avg_col_name] = (
                data[avg_col_name].iloc[i + self._time_window]
                * (self._time_window - step)
                + data[abs_col_name].iloc[i + self._time_window + step]
            ) / self._time_window

    @Instrumentation.trace(name="RsiOldCalculationWorker")
    def add_calculated_columns(self, data):
        identifier_grouped_data = data.groupby(self.get_group_cols(data.columns))
        data[CalculatedColumns.ClosePriceDiff] = identifier_grouped_data[
            BaseColumns.Close
        ].transform(lambda x: x.diff(1))
        data[CalculatedColumns.Gain] = identifier_grouped_data[
            CalculatedColumns.ClosePriceDiff
        ].transform(lambda x: x.clip(lower=0).round(2))
        data[CalculatedColumns.Loss] = identifier_grouped_data[
            CalculatedColumns.ClosePriceDiff
        ].transform(lambda x: x.clip(upper=0).abs().round(2))

        data[CalculatedColumns.ClosePriceDiff].fillna(0)
        data[CalculatedColumns.Gain].fillna(0)
        data[CalculatedColumns.Loss].fillna(0)

        # Get initial Averages
        data[CalculatedColumns.AvgGain] = identifier_grouped_data[
            CalculatedColumns.Gain
        ].transform(
            lambda x: x.rolling(
                window=self._time_window, min_periods=self._time_window
            ).mean()
        )
        data[CalculatedColumns.AvgLoss] = identifier_grouped_data[
            CalculatedColumns.Loss
        ].transform(
            lambda x: x.rolling(
                window=self._time_window, min_periods=self._time_window
            ).mean()
        )

        for identifier in data[BaseColumns.Identifier].unique():
            self.calculate_wsm_average(
                data,
                identifier_grouped_data.get_group(identifier),
                CalculatedColumns.AvgGain,
                CalculatedColumns.Gain,
            )
            self.calculate_wsm_average(
                data,
                identifier_grouped_data.get_group(identifier),
                CalculatedColumns.AvgLoss,
                CalculatedColumns.Loss,
            )

        data[CalculatedColumns.RelativeStrength] = (
            data[CalculatedColumns.AvgGain] / data[CalculatedColumns.AvgLoss]
        )
        data[CalculatedColumns.RelativeStrengthIndex] = 100 - (
            100 / (1.0 + data[CalculatedColumns.RelativeStrength])
        )


class RsiCalculationWorker(CalculationWorker):
    def __init__(self, time_window: int = 14):
        super().__init__(time_window = int(time_window))
        self._columns.append(CalculatedColumns.RelativeStrengthIndex)

    def calculate_rsi(self, group):
        group[CalculatedColumns.RelativeStrengthIndex] = ta.rsi(
            group[BaseColumns.Close]
        )
        return group

    @Instrumentation.trace(name="RsiCalculationWorker")
    def add_calculated_columns(self, data):
        result = data.groupby(self.get_group_cols(data.columns), group_keys=True).apply(
            self.calculate_rsi
        )
        return result.reset_index(drop=True)


class StochRsiCalculationWorker(CalculationWorker):
    def __init__(self, time_window: int = 14):
        super().__init__(time_window = int(time_window))
        self._columns.append(CalculatedColumns.StochRsi_K)
        self._columns.append(CalculatedColumns.StochRsi_D)

    def calculate_stoch_rsi(self, group: pd.DataFrame):
        window = self._params['time_window']
        data = ta.stochrsi(group[BaseColumns.Close], length=window, rsi_length=window, k=3, d=3)
        if data is not None:
            group[CalculatedColumns.StochRsi_K] = data[f"STOCHRSIk_{window}_{window}_3_3"]
            group[CalculatedColumns.StochRsi_D] = data[f"STOCHRSId_{window}_{window}_3_3"]
        return group

    @Instrumentation.trace(name="StochRsiCalculationWorker")
    def add_calculated_columns(self, data):
        result = data.groupby(self.get_group_cols(data.columns), group_keys=True).apply(
            self.calculate_stoch_rsi
        )
        return result.reset_index(drop=True)

class VwapCalculationWorker(CalculationWorker):
    def __init__(self, time_window: int = 1):
        super().__init__(time_window = int(time_window))
        self._columns.append(CalculatedColumns.Vwap)

    @Instrumentation.trace(name="Vwap2CalculationWorker")
    def add_calculated_columns(self, data):
        data[BaseColumns.Turnover] = data[BaseColumns.Turnover].replace("-", 0)
        data[BaseColumns.Volume] = data[BaseColumns.Volume].replace("-", 0)
        if len(data[BaseColumns.Identifier].unique()) > 1:
            data[CalculatedColumns.Vwap] = (
                data.groupby(self.get_group_cols(data.columns))
                .apply(
                    lambda x: x[BaseColumns.Turnover].rolling(self._params['time_window']).sum()
                    / x.rolling(self._params['time_window'])[BaseColumns.Volume].sum()
                )
                .reset_index(level=0, drop=True)
            )
        else:
            data[CalculatedColumns.Vwap] = data[BaseColumns.Turnover].rolling(self._params['time_window']).sum() / data[BaseColumns.Volume].rolling(self._params['time_window']).sum()

class LowestPriceInNextNDaysCalculationWorker(CalculationWorker):
    def __init__(self, N: int = 5, close_price_column: str = BaseColumns.Close, low_price_column: str = BaseColumns.Low):
        super().__init__(N = int(N), close_price_column = close_price_column, low_price_column = low_price_column)
        self._columns.append(f"TroughInNext{str(N)}Sessions")
        self._columns.append(f"TroughPercInNext{str(N)}Sessions")

    @Instrumentation.trace(name="LowestPriceInNextNDaysCalculationWorker")
    def add_calculated_columns(self, data):
        identifier_grouped_data = data.groupby(self.get_group_cols(data.columns))
        data[self._columns[0]] = identifier_grouped_data[
            self._params['low_price_column']
        ].transform(
            lambda x: x.rolling(self._params['N']).min().shift(-self._params['N'])
        )
        data[self._columns[1]] = (
            (data[self._params['close_price_column']] - data[self._columns[0]])
            / data[self._params['close_price_column']]
            * 100
        )

    def get_calculation_window(self) -> CalculationWindow:
        return CalculationWindow(trailing=0, leading=int(self._params['N']))

class HighestPriceInNextNDaysCalculationWorker(CalculationWorker):
    def __init__(self, N: int = 5, close_price_column: str = BaseColumns.Close, high_price_column: str = BaseColumns.High):
        super().__init__(N = int(N), close_price_column = close_price_column, high_price_column = high_price_column)
        self._columns.append(f"PeakInNext{str(N)}Sessions")
        self._columns.append(f"PeakPercInNext{str(N)}Sessions")

    @Instrumentation.trace(name="HighestPriceInNextNDaysCalculationWorker")
    def add_calculated_columns(self, data):
        identifier_grouped_data = data.groupby(self.get_group_cols(data.columns))
        data[self._columns[0]] = identifier_grouped_data[
            self._params['high_price_column']
        ].transform(
            lambda x: x.rolling(self._params['N']).max().shift(-self._params['N'])
        )
        data[self._columns[1]] = (
            (data[self._columns[0]] - data[self._params['close_price_column']])
            / data[self._params['close_price_column']]
            * 100
        )

    def get_calculation_window(self) -> CalculationWindow:
        return CalculationWindow(trailing=0, leading=int(self._params['N']))


class ColumnGrowthCalculationWorker(CalculationWorker):
    def __init__(self, value_column: str = BaseColumns.Close, N: int = 0):
        super().__init__(value_column=value_column, N=int(N))
        self._columns.append(f"{value_column}Growth{str(N)}Sessions")
        self._columns.append(f"{value_column}GrowthPerc{str(N)}Sessions")

    @Instrumentation.trace(name="ColumnGrowthCalculationWorker")
    def add_calculated_columns(self, data: pd.DataFrame):
        identifier_grouped_data = data.groupby(self.get_group_cols(data.columns))
        N: int = self._params['N']
        data[self._columns[0]] = identifier_grouped_data[self._params["value_column"]].transform(lambda x:
            x - x.shift(N)
        )
        data[self._columns[1]] = (
            data[self._columns[0]] / data[self._params["value_column"]] * 100
        )


class ColumnsDeltaCalculationWorker(CalculationWorker):
    def __init__(self, value_column_a: str = None, value_column_b: str = None):
        super().__init__(value_column_a=value_column_a, value_column_b=value_column_b)
        self._columns.append(f"{value_column_a}-{value_column_b}")
        self._columns.append(f"{value_column_a}-{value_column_b}AsPerc")

    @Instrumentation.trace(name="ColumnsDeltaCalculationWorker")
    def add_calculated_columns(self, data: pd.DataFrame):
        data[self._columns[0]] = (
            data[self._params["value_column_a"]] - data[self._params["value_column_b"]]
        )
        data[self._columns[1]] = (
            data[self._columns[0]] / data[self._params["value_column_a"]] * 100
        )