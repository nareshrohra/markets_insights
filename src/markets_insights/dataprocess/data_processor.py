from markets_insights.core.environment import EnvironmentSettings
import os
from string import Template
import pandas as pd
from markets_insights.datareader.data_reader import DataReader, DateRangeSourceDataReader, DateRangeDataReaderWrapper, DateRangeCriteria
from markets_insights.core.column_definition import (
    BaseColumns,
    BasePriceColumns,
    CalculatedColumns,
    PeriodAggregateColumnTemplate,
    AggregationPeriods,
)
from markets_insights.core.core import MarketDaysHelper, Instrumentation, TypeHelper
from markets_insights.core.column_definition import BaseColumns
from markets_insights.calculations.base import (
    CalculationWindow,
    HighestPriceInNextNDaysCalculationWorker,
    LowestPriceInNextNDaysCalculationWorker,
)
from markets_insights.calculations.base import (
    CalculationWorker,
    CalculationPipeline,
    SmaCalculationWorker,
    RsiCalculationWorker,
    BollingerBandCalculationWorker,
    StochRsiCalculationWorker,
    ColumnValueCrossedAboveFlagWorker,
    ColumnValueCrossedBelowFlagWorker,
    PriceCrossedAboveColumnValueFlagWorker,
    PriceCrossedBelowColumnValueFlagWorker,
    StdDevCalculationWorker,
)

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

    def remove_unnamed_columns(self, data: pd.DataFrame) -> pd.DataFrame:
        unnamed_cols = [col for col in data.columns if "Unnamed" in col]
        if len(unnamed_cols):
            data.drop(columns=unnamed_cols, inplace=True)


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

    def create_bb_calculation_pipeline(windows=[200], deviations=[2, 3]):
        pipeline = CalculationPipeline()
        for window in windows:
            pipeline.add_calculation_worker(StdDevCalculationWorker(time_window=window))
            for deviation in deviations:
                worker = BollingerBandCalculationWorker(window, deviation)
                pipeline.add_calculation_worker(worker)
                pipeline.add_calculation_worker(
                    PriceCrossedBelowColumnValueFlagWorker(worker.get_columns()[0])
                )
                pipeline.add_calculation_worker(
                    PriceCrossedAboveColumnValueFlagWorker(worker.get_columns()[1])
                )
        return pipeline

    def create_sma_calculation_pipeline(windows=[50, 100, 200]):
        pipeline = CalculationPipeline()
        for window in windows:
            worker = SmaCalculationWorker(window)
            pipeline.add_calculation_worker(worker)
            pipeline.add_calculation_worker(
                PriceCrossedBelowColumnValueFlagWorker(worker.get_column())
            )
            pipeline.add_calculation_worker(
                PriceCrossedAboveColumnValueFlagWorker(worker.get_column())
            )
        return pipeline

    def create_rsi_calculation_pipeline(
        crossing_above_flag_value=75, crossing_below_flag_value=30, window=14
    ):
        pipeline = CalculationPipeline()
        pipeline.add_calculation_worker(RsiCalculationWorker(window))
        if crossing_above_flag_value is not None:
            pipeline.add_calculation_worker(
                ColumnValueCrossedAboveFlagWorker(
                    CalculatedColumns.RelativeStrengthIndex, crossing_above_flag_value
                )
            )
        if crossing_below_flag_value is not None:
            pipeline.add_calculation_worker(
                ColumnValueCrossedBelowFlagWorker(
                    CalculatedColumns.RelativeStrengthIndex, crossing_below_flag_value
                )
            )
        return pipeline

    def create_stoch_rsi_calculation_pipeline(
        crossing_above_flag_value=80, crossing_below_flag_value=20, window=14
    ):
        pipeline = CalculationPipeline()
        pipeline.add_calculation_worker(StochRsiCalculationWorker(window))
        if crossing_above_flag_value is not None:
            pipeline.add_calculation_worker(
                ColumnValueCrossedAboveFlagWorker(
                    CalculatedColumns.StochRsi_K, crossing_above_flag_value
                )
            )
            pipeline.add_calculation_worker(
                ColumnValueCrossedAboveFlagWorker(
                    CalculatedColumns.StochRsi_D, crossing_above_flag_value
                )
            )
        if crossing_below_flag_value is not None:
            pipeline.add_calculation_worker(
                ColumnValueCrossedBelowFlagWorker(
                    CalculatedColumns.StochRsi_K, crossing_below_flag_value
                )
            )
            pipeline.add_calculation_worker(
                ColumnValueCrossedBelowFlagWorker(
                    CalculatedColumns.StochRsi_D, crossing_below_flag_value
                )
            )
        return pipeline

    def create_forward_looking_price_fall_pipeline(n_days_list):
        forward_looking_lowest_price_pipeline = CalculationPipeline()
        for n in n_days_list:
            forward_looking_lowest_price_pipeline.add_calculation_worker(
                LowestPriceInNextNDaysCalculationWorker(n)
            )
        return forward_looking_lowest_price_pipeline

    def create_forward_looking_price_rise_pipeline(n_days_list):
        forward_looking_highest_price_pipeline = CalculationPipeline()
        for n in n_days_list:
            forward_looking_highest_price_pipeline.add_calculation_worker(
                HighestPriceInNextNDaysCalculationWorker(n)
            )
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

    def get_calculation_window(self) -> CalculationWindow:
        return CalculationWindow.load_from_list([self._store[key].get_calculation_window() for key in self._store])

class HistoricalDataset:
    _daily: pd.DataFrame = None
    _identifier_grouped: pd.core.groupby.DataFrameGroupBy = None
    _monthly: pd.DataFrame = None
    _annual: pd.DataFrame = None

    def create_identifier_grouped(self):
        self._identifier_grouped = self.create_grouped_data(BaseColumns.Identifier)
        return self

    def set_daily_data(self, daily: pd.DataFrame):
        self._daily = daily
        return self

    def set_monthly_data(self, monthly: pd.DataFrame):
        self._monthly = monthly
        return self

    def set_annual_data(self, annual: pd.DataFrame):
        self._annual = annual
        return self

    def create_grouped_data(self, columns):
        return self._daily.groupby(columns)

    def get_identifier_grouped(self):
        return self._identifier_grouped

    def get_daily_data(self) -> pd.DataFrame:
        return self._daily

    def get_monthly_data(self) -> pd.DataFrame:
        return self._monthly

    def get_annual_data(self) -> pd.DataFrame:
        return self._annual

class HistoricalDataProcessOptions:
    def __init__(
        self, include_monthly_data: bool = True, include_annual_data: bool = True
    ):
        self.include_monthly_data = include_monthly_data
        self.include_annual_data = include_annual_data


class HistoricalDataProcessor(DataProcessor):
    options: HistoricalDataProcessOptions

    def __init__(
        self, options: HistoricalDataProcessOptions = HistoricalDataProcessOptions()
    ):
        self.output_dir_template = Template(
            "$DataBaseDir/$ProcessedDataDir/$HistoricalDataDir/"
        )
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


    @Instrumentation.trace(name="HistoricalDataProcessor.process")
    def process(self, reader: DataReader, criteria: DateRangeCriteria) -> HistoricalDataset:
        from_date = MarketDaysHelper.get_this_or_next_market_day(criteria.from_date)
        to_date = MarketDaysHelper.get_this_or_previous_market_day(criteria.to_date)

        daily_data = pd.DataFrame(self.get_data(reader, from_date, to_date).drop_duplicates())

        if not daily_data.empty:
            if reader.filter:
                daily_data = pd.DataFrame(daily_data.query(reader.filter.get_query()))

            self.dataset = HistoricalDataset()
            self.dataset.set_daily_data(daily_data)
            self.dataset.create_identifier_grouped()

            output_path = self.output_dir_template.substitute(**EnvironmentSettings.Paths)

            if self.options.include_monthly_data == True:
                self.dataset.set_monthly_data(self.add_monthly_growth_calc(daily_data))
                monthly_data_filename = self.monthly_group_data_filename.substitute(
                    {**EnvironmentSettings.Paths, "ReaderName": reader.name}
                )
                self.dataset.get_monthly_data().to_csv(output_path + monthly_data_filename)

            if self.options.include_annual_data == True:
                self.dataset.set_annual_data(self.add_yearly_growth_calc(daily_data))
                annual_data_filename = self.annual_group_data_filename.substitute(
                    {**EnvironmentSettings.Paths, "ReaderName": reader.name}
                )
                self.dataset.get_annual_data().to_csv(output_path + annual_data_filename)
        else:
            self.dataset = HistoricalDataset()
            self.dataset.set_daily_data(daily_data)

        return self.dataset
    

    def add_periodic_growth_calc(
        self, processed_data: pd.DataFrame, period: str
    ) -> pd.DataFrame:
        Instrumentation.debug(f"Started periodic calculation for {period}")

        periodic_grouped = processed_data.groupby([BaseColumns.Identifier, period])

        processed_data[
            PeriodAggregateColumnTemplate.substitute(
                {"period": period, "col_name": BaseColumns.Open}
            )
        ] = periodic_grouped[BaseColumns.Open].transform(lambda x: x.iloc[0])
        processed_data[
            PeriodAggregateColumnTemplate.substitute(
                {"period": period, "col_name": BaseColumns.Close}
            )
        ] = periodic_grouped[BaseColumns.Close].transform(lambda x: x.iloc[-1])
        processed_data[
            PeriodAggregateColumnTemplate.substitute(
                {"period": period, "col_name": BaseColumns.Low}
            )
        ] = periodic_grouped[BaseColumns.Low].transform("min")
        processed_data[
            PeriodAggregateColumnTemplate.substitute(
                {"period": period, "col_name": BaseColumns.High}
            )
        ] = periodic_grouped[BaseColumns.High].transform("max")

        processed_data[
            PeriodAggregateColumnTemplate.substitute(
                {"period": period, "col_name": BaseColumns.Volume}
            )
        ] = periodic_grouped[BaseColumns.Volume].transform("sum")
        processed_data[
            PeriodAggregateColumnTemplate.substitute(
                {"period": period, "col_name": BaseColumns.Turnover}
            )
        ] = periodic_grouped[BaseColumns.Turnover].transform("sum")

        periodic_data = periodic_grouped.last().reset_index()

        aggregated_cols = [
            BaseColumns.Volume,
            BaseColumns.Turnover,
        ] + TypeHelper.get_class_static_values(BasePriceColumns)

        periodic_data = periodic_data[
            [BaseColumns.Identifier, BaseColumns.Date, period]
            + [
                PeriodAggregateColumnTemplate.substitute(
                    {"period": period, "col_name": col_name}
                )
                for col_name in aggregated_cols
            ]
        ]

        cols = {}
        for col_name in aggregated_cols:
            cols[
                PeriodAggregateColumnTemplate.substitute(
                    {"period": period, "col_name": col_name}
                )
            ] = col_name
        periodic_data.rename(columns=cols, inplace=True)

        return periodic_data

    @Instrumentation.trace(name="HistoricalDataProcessor.add_yearly_growth_calc")
    def add_yearly_growth_calc(self, processed_data):
        processed_data[CalculatedColumns.Year] = processed_data[
            BaseColumns.Date
        ].dt.year
        return self.add_periodic_growth_calc(processed_data, CalculatedColumns.Year)

    @Instrumentation.trace(name="HistoricalDataProcessor.add_monthly_growth_calc")
    def add_monthly_growth_calc(self, processed_data):
        processed_data[CalculatedColumns.Month] = processed_data[
            BaseColumns.Date
        ].dt.strftime("%Y-%m")
        return self.add_periodic_growth_calc(processed_data, CalculatedColumns.Month)

    def rename_columns(self, reader: DataReader, historical_data: pd.DataFrame):
        column_name_mappings = reader.get_column_name_mappings()
        if column_name_mappings is not None:
            historical_data.rename(columns=column_name_mappings, inplace=True)

    @Instrumentation.trace(name="HistoricalDataProcessor.get_data")
    def get_data(
        self, reader: DataReader, from_date: date, to_date: date
    ):
        Instrumentation.debug("Started to read data")
        output_file = os.path.join(
            self.output_dir_template.substitute(**EnvironmentSettings.Paths),
            self.filename_template.substitute(**{"ReaderName": reader.name}),
        )

        if isinstance(reader, DateRangeSourceDataReader):
            dateRangeReader = reader
        else:
            dateRangeReader = DateRangeDataReaderWrapper(reader)

        save_to_file = False

        Instrumentation.info(f"Reading data from {from_date} to {to_date}")
        read_data = dateRangeReader.read(DateRangeCriteria(from_date, to_date))
        
        if not read_data.empty:
            if save_to_file == True:
                Instrumentation.debug(f"Saving data to file: {output_file}")
                read_data.to_csv(output_file, index=False)

            try:
                read_data[BaseColumns.Date] = read_data[BaseColumns.Date].str.replace(' 00:00:00', '')
            except:
                pass

            read_data[BaseColumns.Date] = pd.to_datetime(read_data[BaseColumns.Date], format="%Y-%m-%d")
            
            read_data = read_data[
                read_data[BaseColumns.Date].dt.date.between(from_date, to_date)
            ]
        
        return read_data
