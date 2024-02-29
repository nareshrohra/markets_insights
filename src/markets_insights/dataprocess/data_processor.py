from markets_insights.core.environment import EnvironmentSettings
import os
from string import Template
import pandas as pd
from markets_insights.datareader.data_reader import DataReader, DateRangeDataReader, DateRangeCriteria
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


class ManualDataProcessor(DataProcessor):
    @Instrumentation.trace(name="ManualDataProcessor.process")
    def process(self, reader: DataReader):
        dir_path = os.path.join(
            self.manual_data_dir_template.substitute(**EnvironmentSettings.Paths),
            reader.name,
        )

        Instrumentation.debug(dir_path)
        csv_files = glob.glob(os.path.join(dir_path, "*.csv"))

        dfs = []
        for file in csv_files:
            df = pd.read_csv(file)

            filename = file.split(os.path.sep)[-1].split(".")[0]
            column_value = filename.replace("_Data", "")

            df["Identifier"] = column_value

            dfs.append(df)

        final_df = pd.concat(dfs)

        final_df[BaseColumns.Date] = pd.to_datetime(final_df[BaseColumns.Date])

        output_file = os.path.join(
            self.manual_data_dir_template.substitute(**EnvironmentSettings.Paths),
            self.filename_template.substitute(**{"ReaderName": reader.name}),
        )

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


    @Instrumentation.trace(name="HistoricalDataProcessor.sanitize_data")
    def sanitize_data(self, data: pd.DataFrame):
        if data.empty:
            return data
        
        self.remove_unnamed_columns(data)
        
        if not BaseColumns.PreviousClose in data.columns:
            data[BaseColumns.PreviousClose] = data.groupby(
                BaseColumns.Identifier
            )[BaseColumns.Close].transform(lambda x: x.shift(-1))
        for col_name in [BaseColumns.Open, BaseColumns.High, BaseColumns.Low]:
            data[col_name] = data.apply(
                lambda x: x[col_name]
                if str(x[col_name]).replace(".", "").isnumeric() == True
                else x[BaseColumns.Close],
                axis=1,
            )
            data[col_name] = data[col_name].astype(float)
        
        for missing_col in [col for col in [BaseColumns.Volume, BaseColumns.Turnover] if col not in data.columns]:
            data[missing_col] = 0

    @Instrumentation.trace(name="HistoricalDataProcessor.process")
    def process(self, reader: DataReader, criteria: DateRangeCriteria) -> HistoricalDataset:
        from_date = MarketDaysHelper.get_this_or_next_market_day(criteria.from_date)
        to_date = MarketDaysHelper.get_this_or_previous_market_day(criteria.to_date)

        daily_data = self.get_data(reader, from_date, to_date)
        manual_data = self.get_manual_data(reader, from_date, to_date)

        if manual_data is not None:
            daily_data = (
                pd.concat([daily_data, manual_data], ignore_index=True)
                .reset_index(drop=True)
                .sort_values(BaseColumns.Date)
            )

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

    @Instrumentation.trace(name="HistoricalDataProcessor.get_manual_data")
    def get_manual_data(
        self, reader: DataReader, from_date: date, to_date: date
    ):
        manual_data_file = os.path.join(
            self.manual_data_dir_template.substitute(**EnvironmentSettings.Paths),
            self.filename_template.substitute(**{"ReaderName": reader.name}),
        )

        if os.path.exists(manual_data_file):
            manual_data = pd.read_csv(manual_data_file)
            manual_data[BaseColumns.Date] = pd.to_datetime(
                manual_data[BaseColumns.Date]
            )
            manual_data = manual_data[
                manual_data[BaseColumns.Date].dt.date.between(from_date, to_date)
            ]

            self.sanitize_data(manual_data)

            return manual_data
        else:
            return None

    @Instrumentation.trace(name="HistoricalDataProcessor.get_data")
    def get_data(
        self, reader: DataReader, from_date: date, to_date: date
    ):
        Instrumentation.debug("Started to read data")
        output_file = os.path.join(
            self.output_dir_template.substitute(**EnvironmentSettings.Paths),
            self.filename_template.substitute(**{"ReaderName": reader.name}),
        )

        if (
            reader.options is not None
            and reader.options.cutoff_date is not None
        ):
            if to_date < reader.options.cutoff_date:
                return pd.DataFrame()
            
            if from_date < reader.options.cutoff_date:
                Instrumentation.debug(
                    f"Reading from {reader.options.cutoff_date} instead of {from_date}"
                )
                from_date = reader.options.cutoff_date

        if isinstance(reader, DateRangeDataReader):
            dateRangeReader = reader
        else:
            dateRangeReader = DateRangeDataReader(reader)

        save_to_file = False

        if os.path.exists(output_file):
            historical_data = pd.read_csv(output_file)
            earliest = pd.Timestamp(historical_data[BaseColumns.Date].min()).date()
            latest = pd.Timestamp(historical_data[BaseColumns.Date].max()).date()
            if earliest > from_date:
                Instrumentation.info(f"Reading data from {from_date} to {earliest}")
                read_data = dateRangeReader.unset_filter().read(DateRangeCriteria(from_date, earliest))
                dateRangeReader.reset_filter()
                historical_data = pd.concat(
                    [historical_data, read_data], ignore_index=True
                ).reset_index(drop=True)
                self.sanitize_data(historical_data)
                save_to_file = True

            if latest < to_date:
                Instrumentation.info(f"Reading data from {latest} to {to_date}")
                read_data = dateRangeReader.unset_filter().read(DateRangeCriteria(latest, to_date))
                dateRangeReader.reset_filter()
                historical_data = pd.concat(
                    [historical_data, read_data], ignore_index=True
                ).reset_index(drop=True)
                self.sanitize_data(historical_data)
                save_to_file = True
        else:
            Instrumentation.info(f"Reading data from {from_date} to {to_date}")
            historical_data = dateRangeReader.unset_filter().read(DateRangeCriteria(from_date, to_date))
            dateRangeReader.reset_filter()
            self.sanitize_data(historical_data)
            save_to_file = True
        
        if save_to_file == True:
            Instrumentation.debug(f"Saving data to file: {output_file}")
            historical_data.to_csv(output_file, index=False)

        try:
            historical_data[BaseColumns.Date] = historical_data[BaseColumns.Date].str.replace(' 00:00:00', '')
        except:
            pass
            #Instrumentation.info(f"Warning: Couldn't remove timestamp")

        historical_data[BaseColumns.Date] = pd.to_datetime(historical_data[BaseColumns.Date], format="%Y-%m-%d")
        
        historical_data = historical_data[
            historical_data[BaseColumns.Date].dt.date.between(from_date, to_date)
        ]
        
        return historical_data
