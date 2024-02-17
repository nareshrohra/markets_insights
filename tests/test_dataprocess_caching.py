import pytest
import pandas as pd
import os
import shutil

from helper import check_col_values, setup, check_base_cols_present, Presets
from markets_insights.core.core import IdentifierFilter
from markets_insights.core.column_definition import BaseColumns
from markets_insights.core.environment import Environment, EnvironmentSettings

from markets_insights.datareader.data_reader import NseIndicesReader
from markets_insights.dataprocess.data_processor import (
    HistoricalDataProcessOptions,
    HistoricalDataProcessor,
    HistoricalDataset,
)

data_folder_path = os.path.join(os.getcwd(), 'data')

def clean_data_folder():
    shutil.rmtree(data_folder_path)

if os.path.isdir(data_folder_path):
    clean_data_folder()
else:
    os.mkdir(data_folder_path)

old_path = EnvironmentSettings.Paths["DataBaseDir"]
Environment.setup(cache_data_base_path=data_folder_path)

setup()

@pytest.mark.parametrize("identifier,unique_ids_count",
    [
        ("Nifty 50", 1),
        (None, 108)        
    ]
)
def test_historical_data_processor_caching_without_filter(identifier: str, unique_ids_count: int):
    processor = HistoricalDataProcessor(HistoricalDataProcessOptions(include_annual_data=False, include_monthly_data=False))
    reader = NseIndicesReader()
    
    if identifier:
        reader.set_filter(IdentifierFilter(identifier))

    result: HistoricalDataset = processor.process(
        reader,
        {"from_date": Presets.dates.dec_start, "to_date": Presets.dates.dec_end},
    )
    unique_ids = result.get_daily_data()[BaseColumns.Identifier].unique()
    assert len(unique_ids) == unique_ids_count

Environment.setup(cache_data_base_path=old_path)