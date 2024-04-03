import math

from attr import dataclass
import markets_insights as mi

from urllib.request import urlopen
from zipfile import ZipFile
from markets_insights.core.column_definition import (
    BaseColumns,
    BasePriceColumns,
    DerivativesBaseColumns,
)
from markets_insights.core.environment import EnvironmentSettings
import os
from string import Template
import pandas as pd
from urllib.error import HTTPError
from datetime import date, timedelta

from markets_insights.core.core import (
    FilterBase,
    InstrumentTypeFilter,
    MarketDaysHelper,
    Instrumentation,
    TypeHelper,
)

from enum import Enum
class Status(Enum):
    NONE: int = 0
    PARTIAL: int = 1
    COMPLETE: int = 2
    UKNOWN: int = 3


class ReaderDateCriteria:
    pass

@dataclass
class ForDateCriteria(ReaderDateCriteria):
    for_date: date


@dataclass
class DateRangeCriteria(ReaderDateCriteria):
    from_date: date
    to_date: date


@dataclass
class MultiDatesCriteria(ReaderDateCriteria):
    for_dates: list[date]


@dataclass(init=False)
class ReaderDataAvailability:
    from_date: date = None
    till_date: date = None

    def __init__(self, from_date: date = date(1900, 1, 1), till_date: date = date.today()):
        self.from_date = from_date
        self.till_date = till_date


@dataclass
class ReaderDataAvailabilityStatus:
    status: int
    availability_ranges: list[DateRangeCriteria] = None
    unavailability_ranges: list[DateRangeCriteria] = None


class ReaderOptions:
    filename = Template("")
    url_template = Template("")
    output_path_template = Template("")
    unzip_path_template = Template("")
    primary_data_path_template = Template("")
    unzip_file: bool = True
    data_availability: list [DateRangeCriteria] = None
    download_timeout = 5  # seconds
    col_prefix = None

@dataclass
class ReaderRescaleOptions:
    volume_scale: int = 1
    turnover_scale: int = 1

def get_safe_min_date(for_date) -> date:
    return for_date if for_date is not None else date(1900, 1, 1)


def get_safe_max_date(for_date) -> date:
    return for_date if for_date is not None else date.today()


class DataReader:
    options: ReaderOptions

    def __init__(self):
        self.options: ReaderOptions = ReaderOptions()
        self.rescale_options: ReaderRescaleOptions = ReaderRescaleOptions()
        self.name: str = ""
        self.filter: FilterBase = None

    def merge_intervals(intervals: list[DateRangeCriteria]) -> list[DateRangeCriteria]:
        merged_ranges: list[DateRangeCriteria] = []
        intervals.sort(key=lambda x: x.from_date)
        for current_range in intervals:
            if not merged_ranges:
                merged_ranges.append(current_range)
            else:
                last_range = merged_ranges[-1]
                if current_range.from_date <= last_range.to_date + timedelta(days=1) or \
                current_range.from_date - last_range.to_date <= timedelta(days=4):
                    merged_ranges[-1] = DateRangeCriteria(
                        from_date=last_range.from_date,
                        to_date=max(last_range.to_date, current_range.to_date)
                    )
                else:
                    merged_ranges.append(current_range)
        return merged_ranges

    def has_data_for_range(data_availability: list[DateRangeCriteria], criteria: DateRangeCriteria):
        read_criteria = DateRangeCriteria(MarketDaysHelper.get_this_or_next_market_day(criteria.from_date), MarketDaysHelper.get_this_or_previous_market_day(criteria.to_date))

        data_availability = DataReader.merge_intervals(data_availability)

        availability_ranges = []
        unavailability_ranges = []
        last_date_processed = read_criteria.from_date - timedelta(days=1)
        
        for interval in data_availability:
            if interval.from_date > read_criteria.to_date:
                break
            if interval.to_date < read_criteria.from_date:
                continue
            
            if interval.from_date > last_date_processed + timedelta(days=1):
                unavailability_ranges.append(DateRangeCriteria(last_date_processed + timedelta(days=1), interval.from_date - timedelta(days=1)))
            
            start_date = max(interval.from_date, read_criteria.from_date)
            end_date = min(interval.to_date, read_criteria.to_date)
            availability_ranges.append(DateRangeCriteria(start_date, end_date))
            last_date_processed = max(last_date_processed, interval.to_date)

        if last_date_processed < read_criteria.to_date:
            unavailability_ranges.append(DateRangeCriteria(last_date_processed + timedelta(days=1), read_criteria.to_date))
        
        unavailability_ranges = DataReader.merge_intervals(unavailability_ranges)

        if not availability_ranges:
            status = Status.NONE
        elif not unavailability_ranges and availability_ranges[0].from_date <= read_criteria.from_date and availability_ranges[-1].to_date >= read_criteria.to_date:
            status = Status.COMPLETE
        else:
            status = Status.PARTIAL

        return ReaderDataAvailabilityStatus(status=status, availability_ranges=availability_ranges, unavailability_ranges=unavailability_ranges)

    def has_data(self, criteria: ReaderDateCriteria) -> ReaderDataAvailabilityStatus:
        if self.options.data_availability:
            if isinstance(criteria, ForDateCriteria):
                status = ReaderDataAvailabilityStatus(status=Status.NONE)
                for availability_range in self.options.data_availability:
                    if availability_range.from_date <= criteria.for_date <= availability_range.to_date:
                        status = ReaderDataAvailabilityStatus(status=Status.COMPLETE, availability_ranges=[DateRangeCriteria(criteria.for_date, criteria.for_date)])
                return status
            elif isinstance(criteria, DateRangeCriteria):
                return DataReader.has_data_for_range(self.options.data_availability, criteria)
        else:
            return ReaderDataAvailabilityStatus(status=Status.UKNOWN)        

    def set_filter(self, filter: FilterBase):
        self.filter = filter
        return self

    def get_date_parts(self, for_date: date):
        return {
            "year": str(for_date.year),
            "month": for_date.strftime("%B").upper()[:3],
            "day": str(for_date.strftime("%d")),
        }

    def merge_with_operation_on_price(
        self, data_l: pd.DataFrame, data_r: pd.DataFrame, operation
    ) -> pd.DataFrame:
        merged_df = pd.merge(
            data_l,
            data_r,
            how="inner",
            on=[BaseColumns.Identifier, BaseColumns.Date],
            left_index=False,
            right_index=False,
        )

        for col in TypeHelper.get_class_static_values(BasePriceColumns):
            merged_df[col] = operation(
                merged_df(merged_df[col + "_x"], merged_df[col + "_y"])
            )

        return merged_df

    def read(self, criteria: ReaderDateCriteria) -> pd.DataFrame:
        data = self.read_data(criteria.for_date)
        return self.post_read_data(data)
    
    def post_read_data(self, data: pd.DataFrame) -> pd.DataFrame:
        if not (data is None or data.empty):
            column_name_mappings = self.get_column_name_mappings()
            if column_name_mappings is not None:
                data.rename(columns=column_name_mappings, inplace=True)

            data.drop_duplicates(inplace=True)

            self.normalise_base_column_values(data)
            
            if self.filter:
                data = data.query(str(self.filter))

            return self.sanitize_data(data)
        else:
            return data

    def read_data(self, for_date: date) -> pd.DataFrame:
        date_parts = self.get_date_parts(for_date)
        filenames = self.get_filenames(for_date)
        output_file_path = self.options.output_path_template.substitute(
            **({**EnvironmentSettings.Paths, **filenames})
        )
        primary_data_file_path = self.options.primary_data_path_template.substitute(
            **({**EnvironmentSettings.Paths, **filenames})
        )

        if not os.path.exists(output_file_path):
            Instrumentation.debug(f"Downloading data for {for_date}")
            url = self.options.url_template.substitute(**({**date_parts, **filenames}))

            Instrumentation.debug(url)

            urldata = urlopen(url, timeout=self.options.download_timeout)
            with open(output_file_path, "wb") as output:
                output.write(urldata.read())

        unzip_folder_path = self.options.unzip_path_template.substitute(
            **({**EnvironmentSettings.Paths, **filenames})
        )
        if self.options.unzip_file == True and not os.path.exists(unzip_folder_path):
            self.unzip_content(output_file_path, unzip_folder_path, for_date)

        data = self.read_data_from_file(for_date, primary_data_file_path)

        return data

    def normalise_base_column_values(self, data: pd.DataFrame) -> pd.DataFrame:
        for col_name in [BaseColumns.Open, BaseColumns.High, BaseColumns.Low]:
            data[col_name] = data.apply(
                lambda x: x[col_name]
                if str(x[col_name]).replace(".", "").isnumeric() == True
                else x[BaseColumns.Close],
                axis=1,
            )
            data[col_name] = data[col_name].astype(float)

        data[BaseColumns.PreviousClose] = data.groupby(
            BaseColumns.Identifier
        )[BaseColumns.Close].transform(lambda x: x.shift(1))

        for col_name in [BaseColumns.Volume, BaseColumns.Turnover]:
            if col_name in data.columns:
                data[col_name] = (
                    data[col_name]
                    .apply(
                        lambda val: val
                        if str(val).replace(".", "").isnumeric() == True
                        else math.nan
                    )
                    .astype(float)
                )
            else:
                data[col_name] = 0

        for rescaling in [
            (BaseColumns.Volume, self.rescale_options.volume_scale),
            (BaseColumns.Turnover, self.rescale_options.turnover_scale),
        ]:
            if rescaling[1] != 1:
                data[rescaling[0]] = data[rescaling[0]] * rescaling[1]

        return data

    def unzip_content(self, output_file_path, unzip_folder_path, for_date):
        zf = ZipFile(output_file_path)
        zf.extractall(path=unzip_folder_path)
        zf.close()

    def read_data_from_file(self, for_date, primary_data_filepath):
        primary_data = pd.read_csv(primary_data_filepath)
        primary_data["Date"] = pd.to_datetime(for_date)
        return primary_data

    def get_filenames(self, for_date):
        return Exception("Not implemented!")

    def get_column_name_mappings(self):
        return None

    def sanitize_data(self, data):
        return data

    def __sub__(self, other):
        return ArithmaticOpReader(
            left=self, right=other, operator=lambda x, y: x - y, op_symbol="-"
        )

    def __add__(self, other):
        return ArithmaticOpReader(
            left=self, right=other, operator=lambda x, y: x + y, op_symbol="+"
        )

    def __mul__(self, other):
        return ArithmaticOpReader(
            left=self, right=other, operator=lambda x, y: x * y, op_symbol="*"
        )

    def __truediv__(self, other):
        return ArithmaticOpReader(
            left=self, right=other, operator=lambda x, y: x / y, op_symbol="/"
        )


def get_date_criteria_based_reader(reader: DataReader, criteria: ReaderDateCriteria) -> DataReader:
    if isinstance(criteria, DateRangeCriteria) and not isinstance(reader, DateRangeSourceDataReader):
        return DateRangeDataReaderWrapper(reader)
    elif isinstance(criteria, MultiDatesCriteria) and not isinstance(reader, MultiDatesDataReader):
        return MultiDatesDataReader(reader)
    else:
        return reader
    

class MultiDatesDataReader(DataReader):
    reader: DataReader

    def __init__(self, reader: DataReader):
        super().__init__()
        self.reader = reader
        self.name = reader.name
        self.filter = reader.filter
        self.options = reader.options

    def read(self, criteria: ReaderDateCriteria):
        if not isinstance(criteria, MultiDatesCriteria):
            raise Exception("MultiDatesDataReader.read() expects MultiDatesCriteria")
        
        result = pd.DataFrame()
        for for_date in criteria.for_dates:
            if MarketDaysHelper.is_open_for_day(pd.Timestamp(for_date).date()):
                try:
                    data = self.reader.read(ForDateCriteria(for_date))
                    
                    if result.empty:
                        result = data
                    else:
                        if not data.empty:
                            result = pd.concat(
                                [result, data], ignore_index=True
                            ).reset_index(drop=True)
                except Exception as e:
                    print(e, for_date.strftime("date(%Y, %m, %d),"))

        return self.post_read_data(result)


class SingleDaySourceDataReader(DataReader):
    pass


class DateRangeSourceDataReader(DataReader):
    pass

class DateRangeDataReaderWrapper(DateRangeSourceDataReader):
    reader: DataReader

    def __init__(self, reader: DataReader):
        super().__init__()
        if reader:
            self.name = reader.name
            self.filter = reader.filter
            self.options = reader.options
            self.reader = reader

    def read(self, criteria: ReaderDateCriteria):
        if not isinstance(criteria, ReaderDateCriteria):
            raise Exception("DateRangeDataReader.read() expects ReaderDateCriteria")
        
        datelist = MarketDaysHelper.get_days_list_for_range(criteria.from_date, criteria.to_date)
        result = pd.DataFrame()
        for for_date in datelist:
            if MarketDaysHelper.is_open_for_day(pd.Timestamp(for_date).date()):
                try:
                    data = self.reader.read(ForDateCriteria(for_date))
                    
                    if result.empty:
                        result = data
                    else:
                        if not data.empty:
                            result = pd.concat(
                                [result, data], ignore_index=True
                            ).reset_index(drop=True)
                except Exception as e:
                    print(e, for_date.strftime("date(%Y, %m, %d),"))

        return self.post_read_data(result)

    def has_data(self, criteria: ReaderDateCriteria):
        if self.reader:
            return self.reader.has_data(criteria)
        else:
            return self.has_data()


class ChainedDataReader(DateRangeSourceDataReader):
    def __init__(self, next: DataReader):
        super().__init__()
        if not isinstance(next, DateRangeSourceDataReader):
            self.next = DateRangeDataReaderWrapper(next)
        else:
            self.next = next
    
    Instrumentation.trace("ChainedDataReader.read")
    def read(self, criteria: ReaderDateCriteria) -> pd.DataFrame:
        # check has data for date range
        availability: ReaderDataAvailabilityStatus = self.has_data(criteria)
        
        if availability.status == Status.COMPLETE:
            Instrumentation.info(f"Data availability {self.__class__}: {str(availability.status)}, criteria: {str(criteria)}")
            data = self.read_data(criteria)
        elif availability.status == Status.NONE or availability.status == Status.UKNOWN:
            Instrumentation.info(f"Data availability {self.__class__}: {str(availability.status)}, criteria: {str(criteria)}")
            data = self.next.read(criteria)
            if not data.empty:
                #data = self.post_read_data(data)
                self.on_received_more_data(data)
        elif availability.status == Status.PARTIAL:
            Instrumentation.info(f"{self.__class__} -> Data availability {str(availability.status)}, criteria: {str(criteria)}")
            available_data = self.read_data(criteria)
            unavailable_data: list[pd.DataFrame] = []
            
            for date_range in availability.unavailability_ranges:
                Instrumentation.info(f"{self.__class__} -> reading unavailability range: {str(criteria)}")
                data = self.next.read(date_range)
                if not data.empty:
                    unavailable_data.append(data)
            if len(unavailable_data) > 0:
                all_unavailable_data = pd.concat(unavailable_data).sort_values([BaseColumns.Date])
                #all_unavailable_data = self.post_read_data(all_unavailable_data)
                data = pd.concat([all_unavailable_data, available_data])
                data[BaseColumns.Date] = data[BaseColumns.Date].astype('datetime64[ns]')
                data = data.sort_values([BaseColumns.Date])
                self.on_received_more_data(all_unavailable_data)
            else:
                data = available_data
        
        return self.post_read_data(data)
    
    def on_received_more_data(self, data: list[pd.DataFrame]):
        pass


class CachedDataReader(ChainedDataReader):
    def __init__(self, next: DataReader):
        super().__init__(next)
        self.name = next.name
        self.options.col_prefix = next.options.col_prefix
    
    def read_data(self, criteria) -> pd.DataFrame:
        return self.read_cached_data(criteria)
    
    def read_cached_data(self, criteria) -> pd.DataFrame:
        raise "Not implemented exception"
    
    def post_read_data(self, data: pd.DataFrame) -> pd.DataFrame:
        data = data[data.columns.drop(list(data.filter(regex='Unnamed')))]
        data = data.reset_index(drop=True)
        data = data.drop_duplicates()
        if self.filter:
            data = data.query(str(self.filter))
        return data


class MemoryCachedDataReader(CachedDataReader):
    def __init__(self, next: DataReader):
        super().__init__(next)
        self.cached_data = pd.DataFrame()
    
    def read_cached_data(self, criteria: DateRangeCriteria) -> pd.DataFrame:
        return self.cached_data[
            self.cached_data[BaseColumns.Date].dt.date.between(criteria.from_date, criteria.to_date)
        ]
        
    def on_received_more_data(self, new_data: pd.DataFrame):
        existing_data = self.cached_data
        self.cached_data = pd.concat([existing_data, new_data])
        
        self.cached_data.sort_values(BaseColumns.Date, inplace=True)
        if self.options.data_availability is None:
            self.options.data_availability = []

        self.options.data_availability = self.get_data_availability_ranges()

    def get_data_availability_ranges(self):
        df = pd.DataFrame()
        df['Date'] = self.cached_data['Date']
        df['Gap'] = self.cached_data['Date'].diff().dt.days

        df['NewRange'] = df['Gap'] > 4

        df['Group'] = df['NewRange'].cumsum()

        ranges_df = df.groupby('Group')['Date'].agg(['min', 'max']).reset_index(drop=True)

        availability_ranges: list[DateRangeCriteria] = []
        for index, row in ranges_df.iterrows():
            availability_ranges.append(DateRangeCriteria(row['min'].date(), row['max'].date()))

        return availability_ranges




class ArithmaticOpReader(DataReader):
    def __init__(self, left: DataReader, right: DataReader, operator, op_symbol: str):
        super().__init__()
        self.l_reader = left
        self.r_reader = right
        self.operator = operator
        self.op_symbol = op_symbol
        self.l_prefix = None
        self.r_prefix = None
        self.options.col_prefix = ""
        self.name = f"{left.name}{op_symbol}{right.name}"

    def read(self, criteria: ReaderDateCriteria) -> pd.DataFrame:
        l_data = get_date_criteria_based_reader(self.l_reader, criteria).read(criteria)
        r_data = get_date_criteria_based_reader(self.r_reader, criteria).read(criteria)
        if not (l_data.empty or r_data.empty):
            on_cols = [BaseColumns.Identifier, BaseColumns.Date]
            prefix_l = self.l_reader.options.col_prefix
            prefix_r = self.r_reader.options.col_prefix

            l_data_unique_id_count = len(l_data[BaseColumns.Identifier].unique())
            r_data_unique_id_count = len(r_data[BaseColumns.Identifier].unique())
            
            if l_data_unique_id_count == 1:
                on_cols = [BaseColumns.Date]
                prefix_l = l_data[BaseColumns.Identifier].values[0] + "-"
            
            if r_data_unique_id_count == 1:
                on_cols = [BaseColumns.Date]
                prefix_r = r_data[BaseColumns.Identifier].values[0] + "-"
            
            if not (
                f"{prefix_l}{BaseColumns.Close}" in l_data.columns
                and f"{prefix_r}{BaseColumns.Close}" in l_data.columns
            ):
                merged_df = pd.merge(
                    l_data,
                    r_data,
                    how="inner",
                    on=on_cols,
                    left_index=False,
                    right_index=False,
                )

                if BaseColumns.Identifier not in on_cols:
                    merged_df[BaseColumns.Identifier] = merged_df[BaseColumns.Identifier + "_x"] + " " + self.op_symbol + " " + merged_df[BaseColumns.Identifier + "_y"]

                update_col_prefix = {}
                for col in [col for col in merged_df.columns if "_x" in col]:
                    update_col_prefix[col] = f"{prefix_l}{col.replace('_x', '')}"
                for col in [col for col in merged_df.columns if "_y" in col]:
                    update_col_prefix[col] = f"{prefix_r}{col.replace('_y', '')}"

                merged_df.rename(columns=update_col_prefix, inplace=True)
            else:
                merged_df = l_data

            for col in TypeHelper.get_class_static_values(BasePriceColumns):
                merged_df[col] = self.operator(
                    merged_df[f"{prefix_l}{col}"], merged_df[f"{prefix_r}{col}"]
                )

            self.l_prefix = prefix_l
            self.r_prefix = prefix_r

            return merged_df
        else:
            return pd.DataFrame()


class BhavCopyReader(SingleDaySourceDataReader):
    def __init__(self):
        super().__init__()
        self.name = "nse_equities"
        self.options.col_prefix = "Cash-"
        __base_url = "https://archives.nseindia.com/content/historical/EQUITIES/"
        self.options.data_availability = [DateRangeCriteria(date.fromisoformat("2016-01-01"), date.today())]
        self.options.url_template = Template(
            __base_url + "$year/$month/$download_filename"
        )
        self.options.output_path_template = Template(
            f"$DataBaseDir/$RawDataDir/$BhavDataDir/$download_filename"
        )
        self.options.unzip_path_template = Template(
            "$DataBaseDir/$RawDataDir/$BhavDataDir/$download_filename_wo_ext"
        )
        self.options.primary_data_path_template = Template(
            "$DataBaseDir/$RawDataDir/$BhavDataDir/$download_filename_wo_ext/$primary_data_filename"
        )

    def get_filenames(self, for_date):
        __formatted_date = for_date.strftime("%d%b%Y").upper()
        return {
            "download_filename_wo_ext": f"cm{__formatted_date}bhav",
            "download_filename": f"cm{__formatted_date}bhav.csv.zip",
            "primary_data_filename": f"cm{__formatted_date}bhav.csv",
        }

    def get_column_name_mappings(self):
        return {
            "SYMBOL": BaseColumns.Identifier,
            "PREVCLOSE": BaseColumns.PreviousClose,
            "OPEN": BaseColumns.Open,
            "HIGH": BaseColumns.High,
            "LOW": BaseColumns.Low,
            "CLOSE": BaseColumns.Close,
            "TOTTRDQTY": BaseColumns.Volume,
            "TOTTRDVAL": BaseColumns.Turnover,
        }

    def sanitize_data(self, data):
        return data[data["SERIES"] == "EQ"].reset_index(drop=True)


class NseIndicesNewReader(SingleDaySourceDataReader):
    def __init__(self):
        super().__init__()
        self.name = "nse_indices"
        self.options.col_prefix = "index-"
        self.rescale_options.turnover_scale = math.pow(10, 7)
        __base_url = "https://archives.nseindia.com/content/indices/"
        self.options.unzip_file = False
        self.options.url_template = Template(__base_url + "$download_filename")
        self.options.output_path_template = Template(
            "$DataBaseDir/$RawDataDir/$NseIndicesDataDir/$download_filename"
        )
        self.options.primary_data_path_template = Template(
            "$DataBaseDir/$RawDataDir/$NseIndicesDataDir/$download_filename"
        )
        self.options.data_availability = [DateRangeCriteria(date.fromisoformat("2013-01-01"), date.today())]

    def get_filenames(self, for_date: date):
        __formatted_date = for_date.strftime("%d%m%Y").upper()
        return {
            "download_filename": f"ind_close_all_{__formatted_date}.csv",
            "primary_data_filename": f"ind_close_all_{__formatted_date}.csv",
        }

    def get_column_name_mappings(self):
        return {
            "Index Name": BaseColumns.Identifier,
            "Open Index Value": BaseColumns.Open,
            "High Index Value": BaseColumns.High,
            "Low Index Value": BaseColumns.Low,
            "Closing Index Value": BaseColumns.Close,
            "Volume": BaseColumns.Volume,
            "Turnover (Rs. Cr.)": BaseColumns.Turnover,
        }


class NseDerivatiesReaderBase(SingleDaySourceDataReader):
    def __init__(self):
        super().__init__()
        self.options.data_availability = [DateRangeCriteria(date(2016, 1, 1), date.today())]

    def sanitize_data(self, data):
        return data[data["OpenInterest"] > 0].reset_index(drop=True)


class NseDerivatiesReader(NseDerivatiesReaderBase):
    def __init__(self):
        super().__init__()
        self.name = "nse_derivatives"
        self.options.col_prefix = "FO-"
        self.rescale_options.turnover_scale = math.pow(10, 7)
        self.options.unzip_file = False
        __base_url = "https://archives.nseindia.com/content/fo/"
        self.options.url_template = Template(__base_url + "$download_filename")
        self.options.output_path_template = Template(
            "$DataBaseDir/$RawDataDir/$NseDerivativesDataDir/$download_filename"
        )
        self.options.primary_data_path_template = Template(
            "$DataBaseDir/$RawDataDir/$NseDerivativesDataDir/$download_filename"
        )

    def get_filenames(self, for_date):
        # __formatted_date = for_date.strftime('%d%b%Y').upper()
        __formatted_date = for_date.strftime("%d%m%Y")
        return {
            "download_filename": f"NSE_FO_bhavcopy_{__formatted_date}.csv",
            "primary_data_filename": f"NSE_FO_bhavcopy_{__formatted_date}.csv",
        }

    def get_column_name_mappings(self):
        return {
            "TckrSymb": BaseColumns.Identifier,
            "XpryDt": DerivativesBaseColumns.ExpiryDate,
            "OptnTp": DerivativesBaseColumns.OptionType,
            "PrvsClsgPric": BaseColumns.PreviousClose,
            "TtlTradgVol": BaseColumns.Volume,
            "TtlTrfVal": BaseColumns.Turnover,
            "OpnPric": BaseColumns.Open,
            "HghPric": BaseColumns.High,
            "LwPric": BaseColumns.Low,
            "ClsPric": BaseColumns.Close,
            "OpnIntrst": DerivativesBaseColumns.OpenInterest,
            "PctgChngInOpnIntrst": DerivativesBaseColumns.OiChangePct,
            "StrkPric": DerivativesBaseColumns.StrikePrice,
            "FinInstrmNm": DerivativesBaseColumns.InstrumentType,
        }


class NseEquityFuturesDataReader(NseDerivatiesReader):
    def __init__(self):
        super().__init__()
        self.name = "nse_futstk"
        self.options.col_prefix = "Futures-"
        self.set_filter(InstrumentTypeFilter("FUTSTK"))


class NseDerivatiesOldReader(NseDerivatiesReaderBase):
    def __init__(self):
        super().__init__()
        self.name = "Derivatives"
        self.options.col_prefix = "FO-"
        self.rescale_options.turnover_scale = math.pow(10, 7)
        __base_url = "https://archives.nseindia.com/content/historical/DERIVATIVES/"
        self.options.url_template = Template(
            __base_url + "$year/$month/$download_filename"
        )
        self.options.output_path_template = Template(
            "$DataBaseDir/$RawDataDir/$NseDerivativesDataDir/$download_filename"
        )
        self.options.unzip_path_template = Template(
            "$DataBaseDir/$RawDataDir/$NseDerivativesDataDir/$download_filename_wo_ext"
        )
        self.options.primary_data_path_template = Template(
            "$DataBaseDir/$RawDataDir/$NseDerivativesDataDir/$download_filename_wo_ext/$primary_data_filename"
        )

    def get_filenames(self, for_date):
        __formatted_date = for_date.strftime("%d%b%Y").upper()
        return {
            "download_filename_wo_ext": f"fo{__formatted_date}bhav",
            "download_filename": f"fo{__formatted_date}bhav.csv.zip",
            "primary_data_filename": f"fo{__formatted_date}bhav.csv",
        }

    def get_column_name_mappings(self):
        return {
            "SYMBOL": BaseColumns.Identifier,
            "EXPIRY_DT": DerivativesBaseColumns.ExpiryDate,
            "OPTION_TYP": DerivativesBaseColumns.OptionType,
            "PrvsClsgPric": BaseColumns.PreviousClose,
            "TtlTradgVol": BaseColumns.Turnover,
            "OPEN": BaseColumns.Open,
            "HIGH": BaseColumns.High,
            "LOW": BaseColumns.Low,
            "CLOSE": BaseColumns.Close,
            "OpnIntrst": DerivativesBaseColumns.OpenInterest,
            "PctgChngInOpnIntrst": DerivativesBaseColumns.OiChangePct,
            "STRIKE_PR": DerivativesBaseColumns.StrikePrice,
        }


class NseIndexFuturesDataReader(NseDerivatiesReader):
    def __init__(self):
        super().__init__()
        self.name = "nse_futidx"
        self.options.col_prefix = "Futures-"
        self.set_filter(InstrumentTypeFilter("FUTIDX"))
        
    
    def sanitize_data(self, data: pd.DataFrame):
        mapping = {
            "NIFTY": "Nifty 50",
            "BANKNIFTY": "Nifty Bank",
            "FINNIFTY": "Nifty Financial Services",
        }
        data.loc[data.index, BaseColumns.Identifier] = data[BaseColumns.Identifier].replace(mapping)
        return data


class NseIndexOptionsDataReader(NseDerivatiesReader):
    def __init__(self):
        super().__init__()
        self.name = "nse_optidx"
        self.options.col_prefix = "Option-"
        self.set_filter(InstrumentTypeFilter("OPTIDX"))
    
    def sanitize_data(self, data: pd.DataFrame):
        mapping = {
            "NIFTY": "Nifty 50",
            "BANKNIFTY": "Nifty Bank",
            "FINNIFTY": "Nifty Financial Services",
        }
        data.loc[data.index, BaseColumns.Identifier] = data[BaseColumns.Identifier].replace(mapping)
        return data


class NseEquityOptionsDataReader(NseDerivatiesReader):
    def __init__(self):
        super().__init__()
        self.name = "nse_optstk"
        self.options.col_prefix = "Option-"
        self.set_filter(InstrumentTypeFilter("OPTSTK"))


class NseIndicesManualDataReader(DateRangeSourceDataReader):
    output_dir_template = Template("")
    filename_template = Template("$ReaderName.csv")
    data_dir_template = Template("$DataBaseDir/$ManualDataDir")

    def __init__(self):
        super().__init__()
        self.name = "nse_indices"
        self.options.col_prefix = "index-"
        self.options.data_availability = [DateRangeCriteria(date(1990, 7, 3), date(2012, 12, 31))]

    def read(self, criteria: DateRangeCriteria):
        data_file = os.path.join(
            self.data_dir_template.substitute(**EnvironmentSettings.Paths),
            self.filename_template.substitute(**{"ReaderName": "Indices"}),
        )

        if os.path.exists(data_file):
            data = pd.read_csv(data_file)
            data[BaseColumns.Date] = pd.to_datetime(
                data[BaseColumns.Date]
            )
            data = data[
                data[BaseColumns.Date].dt.date.between(criteria.from_date, criteria.to_date)
            ]
            
            return self.post_read_data(data)
        else:
            return pd.DataFrame()


class NseIndicesReader(ChainedDataReader):
    def __init__(self):
        super().__init__(NseIndicesManualDataReader())
        self.name = "nse_indices"
        self.options.col_prefix = "index-"
        self.new_reader = DateRangeDataReaderWrapper(NseIndicesNewReader())
    
    def read_data(self, criteria: ReaderDateCriteria):
        return self.new_reader.read(criteria)
    
    def has_data(self, criteria: ReaderDateCriteria):
        return self.new_reader.has_data(criteria)