from datetime import date
import pytest
import pandas as pd

from helper import check_col_values, setup, Presets
from markets_insights.calculations.base import (
    ColumnValueBelowFlagWorker,
    ColumnValueAboveFlagWorker,
    ColumnValueCrossedAboveAnotherColumnValueFlagWorker,
    ColumnValueCrossedBelowAnotherColumnValueFlagWorker,
    ColumnValueBelowAnotherColumnValueFlagWorker,
    ColumnValueAboveAnotherColumnValueFlagWorker,
    ColumnsDeltaCalculationWorker,
    ColumnGrowthCalculationWorker,
    PriceCrossedAboveColumnValueFlagWorker,
    PriceCrossedBelowColumnValueFlagWorker,
    SmaCalculationWorker,
    StochRsiCalculationWorker,
    VwapCalculationWorker,
    ColumnsDeltaCalculationWorker,
)
from markets_insights.core.core import DateFilter, IdentifierFilter

setup()

import markets_insights
from markets_insights.core.column_definition import BaseColumns, CalculatedColumns
from markets_insights.datareader.data_reader import BhavCopyReader, NseIndicesReader
from markets_insights.dataprocess.data_processor import (
    CalculationPipelineBuilder,
    HistoricalDataProcessor,
    HistoricalDataset,
    MultiDataCalculationPipelines,
)

indices_processor = HistoricalDataProcessor()
indices_result: HistoricalDataset = indices_processor.process(
    NseIndicesReader(),
    {"from_date": Presets.dates.year_start, "to_date": Presets.dates.year_end},
)

equity_processor = HistoricalDataProcessor()
equity_result: HistoricalDataset = equity_processor.process(
    BhavCopyReader(),
    {"from_date": Presets.dates.q4_start, "to_date": Presets.dates.q4_end},
)


def test_calculations_rsi():
    # Prepare calculation pipeline
    pipelines = MultiDataCalculationPipelines()
    pipelines.set_item(
        "rsi", CalculationPipelineBuilder.create_rsi_calculation_pipeline()
    )
    indices_processor.set_calculation_pipelines(pipelines)

    # Run the pipeline and get data
    indices_processor.run_calculation_pipelines()
    daily_data = indices_result.get_daily_data()

    check_col_values(
        data=daily_data.query(
            str(Presets.filters.nifty50 & Presets.filters.date_year_end)
        ),
        col_value_pairs={CalculatedColumns.RelativeStrengthIndex: 76.39132974363618},
    )
    assert pipelines.get_calculation_window().trailing == 14

def test_calculations_crossing_flags():
    # Prepare calculation pipeline
    pipelines = MultiDataCalculationPipelines()
    pipelines.set_item(
        "rsi", CalculationPipelineBuilder.create_rsi_calculation_pipeline()
    )
    indices_processor.set_calculation_pipelines(pipelines)

    # Run the pipeline and get data
    indices_processor.run_calculation_pipelines()
    daily_data = indices_result.get_daily_data()

    for for_date in [date(2023, 2, 28), date(2023, 10, 26)]:
        check_col_values(
            data=daily_data.query(
                str(Presets.filters.nifty50 & DateFilter(for_date=for_date))
            ),
            col_value_pairs={"RsiCrossedBelow": True},
        )

    for for_date in [
        date(2023, 7, 4),
        date(2023, 9, 15),
        date(2023, 12, 1),
        date(2023, 12, 27),
    ]:
        check_col_values(
            data=daily_data.query(
                str(Presets.filters.nifty50 & DateFilter(for_date=for_date))
            ),
            col_value_pairs={"RsiCrossedAbove": True},
        )
    assert pipelines.get_calculation_window().trailing == 14

def test_calculations_sma():
    # Prepare calculation pipeline
    pipelines = MultiDataCalculationPipelines()
    pipelines.set_item(
        "sma", CalculationPipelineBuilder.create_sma_calculation_pipeline()
    )
    indices_processor.set_calculation_pipelines(pipelines)

    # Run the pipeline and get data
    indices_processor.run_calculation_pipelines()
    daily_data = indices_result.get_daily_data()

    check_col_values(
        data=daily_data.query(
            str(Presets.filters.nifty50 & Presets.filters.date_year_end)
        ),
        col_value_pairs={
            "Sma50": 20171.113,
            "Sma100": 19891.132,
            "Sma200": 19142.190749999998,
        },
    )
    assert pipelines.get_calculation_window().trailing == 200

def test_calculations_stoch_rsi():
    # Prepare calculation pipeline
    pipelines = MultiDataCalculationPipelines()
    stoch_rsi_worker = StochRsiCalculationWorker()
    pipelines.set_item(
        "stoch_rsi",
        CalculationPipelineBuilder.create_pipeline_for_worker(stoch_rsi_worker),
    )
    indices_processor.set_calculation_pipelines(pipelines)

    # Run the pipeline and get data
    indices_processor.run_calculation_pipelines()
    daily_data = indices_result.get_daily_data()

    check_col_values(
        data=daily_data.query(
            str(Presets.filters.nifty50 & Presets.filters.date_year_end)
        ),
        col_value_pairs={
            CalculatedColumns.StochRsi_K: 53.10131491044907,
            CalculatedColumns.StochRsi_D: 45.248290876233135,
        },
    )
    assert pipelines.get_calculation_window().trailing == 14

@pytest.mark.parametrize(
    "identifier,for_date,vwap",
    [
        ("RELIANCE", date(2023, 12, 21), 2383.908377),
        ("INFY", date(2023, 12, 27), 1457.663944),
        ("HDFCBANK", date(2023, 12, 29), 1572.029516),
    ],
)
def test_calculations_vwap(identifier: str, for_date: date, vwap: float):
    # Prepare calculation pipeline
    pipelines = MultiDataCalculationPipelines()
    worker = VwapCalculationWorker(time_window=50)
    pipelines.set_item(
        "vwap", CalculationPipelineBuilder.create_pipeline_for_worker(worker)
    )
    equity_processor.set_calculation_pipelines(pipelines)

    # Run the pipeline and get data
    equity_processor.run_calculation_pipelines()
    daily_data = equity_result.get_daily_data()

    check_col_values(
        data=daily_data.query(str(IdentifierFilter(identifier) & DateFilter(for_date))),
        col_value_pairs={CalculatedColumns.Vwap: vwap},
    )

    assert worker.get_calculation_window().trailing == 50

@pytest.mark.parametrize(
    "identifier,for_date,vwap",
    [
        ("RELIANCE", date(2023, 12, 21), 2383.908377),
        ("INFY", date(2023, 12, 27), 1457.663944),
        ("HDFCBANK", date(2023, 12, 29), 1572.029516),
    ],
)
def test_calculations_vwap_with_identifier(identifier: str, for_date: date, vwap: float):
    processor = HistoricalDataProcessor()
    result: HistoricalDataset = processor.process(
        BhavCopyReader().set_filter(IdentifierFilter(identifier)),
        {"from_date": Presets.dates.year_start, "to_date": Presets.dates.year_end},
    )

    # Prepare calculation pipeline
    pipelines = MultiDataCalculationPipelines()
    worker = VwapCalculationWorker(time_window=50)
    pipelines.set_item(
        "vwap", CalculationPipelineBuilder.create_pipeline_for_worker(worker)
    )
    processor.set_calculation_pipelines(pipelines)

    # Run the pipeline and get data
    processor.run_calculation_pipelines()
    daily_data = result.get_daily_data()

    check_col_values(
        data=daily_data.query(str(DateFilter(for_date))),
        col_value_pairs={CalculatedColumns.Vwap: vwap},
    )
    assert worker.get_calculation_window().trailing == 50


def test_calculations_bb():
    # Prepare calculation pipeline
    pipelines = MultiDataCalculationPipelines()
    pipelines.set_item(
        "bb", CalculationPipelineBuilder.create_bb_calculation_pipeline()
    )
    indices_processor.set_calculation_pipelines(pipelines)

    # Run the pipeline and get data
    indices_processor.run_calculation_pipelines()
    daily_data = indices_result.get_daily_data()

    check_col_values(
        data=daily_data.query(
            str(Presets.filters.nifty50 & Presets.filters.date_year_end)
        ),
        col_value_pairs={
            "StdDev200": 1084.4860011940468,
            "Bb200Dev2Upper": 21311.162752388092,
            "Bb200Dev2Lower": 16973.218747611903,
            "Bb200Dev3Upper": 22395.64875358214,
            "Bb200Dev3Lower": 15888.732746417858,
        },
    )
    assert pipelines.get_calculation_window().trailing == 200


@pytest.mark.parametrize(
    "n_day,lowest_price,price_fall_pert",
    [
        (5, 20507.75, -1.183398378717077),
        (10, 20507.75, -1.183398378717077),
        (15, 20507.75, -1.183398378717077),
    ],
)
def test_calculations_fwd_looking_fall(
    n_day: int, lowest_price: float, price_fall_pert: float
):
    # Prepare calculation pipeline
    pipelines = MultiDataCalculationPipelines()
    pipelines.set_item(
        "fwd_price_fall",
        CalculationPipelineBuilder.create_forward_looking_price_fall_pipeline([n_day]),
    )
    indices_processor.set_calculation_pipelines(pipelines)

    # Run the pipeline and get data
    indices_processor.run_calculation_pipelines()
    daily_data = indices_result.get_daily_data()

    test_day_data = daily_data.query(
        str(Presets.filters.nifty50 & DateFilter(for_date=Presets.dates.dec_start))
    )
    check_col_values(
        data=test_day_data,
        col_value_pairs={
            f"TroughInNext{str(n_day)}Sessions": lowest_price,
            f"TroughPercInNext{str(n_day)}Sessions": price_fall_pert,
        },
    )
    assert pipelines.get_calculation_window().leading == n_day


@pytest.mark.parametrize(
    "n_day,highest_price,price_rise_pert",
    [
        (5, 21006.1, 3.6422125627223196),
        (10, 21492.3, 6.041079736923893),
        (15, 21593.0, 6.537924501305012),
    ],
)
def test_calculations_fwd_looking_rise(
    n_day: int, highest_price: float, price_rise_pert: float
):
    # Prepare calculation pipeline
    pipelines = MultiDataCalculationPipelines()
    pipelines.set_item(
        "fwd_price_rise",
        CalculationPipelineBuilder.create_forward_looking_price_rise_pipeline([n_day]),
    )
    indices_processor.set_calculation_pipelines(pipelines)

    # Run the pipeline and get data
    indices_processor.run_calculation_pipelines()
    daily_data = indices_result.get_daily_data()

    test_day_data = daily_data.query(
        str(Presets.filters.nifty50 & DateFilter(for_date=Presets.dates.dec_start))
    )
    check_col_values(
        data=test_day_data,
        col_value_pairs={
            f"PeakInNext{str(n_day)}Sessions": highest_price,
            f"PeakPercInNext{str(n_day)}Sessions": price_rise_pert,
        },
    )
    assert pipelines.get_calculation_window().leading == n_day

@pytest.mark.parametrize(
    "identifier,for_date,flag",
    [
        ("WONDERLA", date(2023, 12, 14), True),
        ("GAEL", date(2023, 12, 28), True),
        ("RELIANCE", date(2023, 12, 14), False),
        ("INFY", date(2023, 12, 29), False),
    ],
)
def test_calculations_price_crossing_below_flag(
    identifier: str, for_date: date, flag: bool
):
    # Prepare calculation pipeline
    pipelines = MultiDataCalculationPipelines()
    pipelines.set_item(
        "vwap",
        CalculationPipelineBuilder.create_pipeline_for_worker(
            VwapCalculationWorker(time_window=50)
        ),
    )
    pipelines.set_item(
        "vwap_crossed",
        CalculationPipelineBuilder.create_pipeline_for_worker(
            PriceCrossedBelowColumnValueFlagWorker(value_column=CalculatedColumns.Vwap)
        ),
    )
    equity_processor.set_calculation_pipelines(pipelines)

    # Run the pipeline and get data
    equity_processor.run_calculation_pipelines()
    daily_data = equity_result.get_daily_data()

    check_col_values(
        data=daily_data.query(
            str(IdentifierFilter(identifier) & DateFilter(for_date=for_date))
        ),
        col_value_pairs={"PriceCrossedBelowVwap": flag},
    )
    assert pipelines.get_calculation_window().trailing == 50


@pytest.mark.parametrize(
    "identifier,for_date,flag",
    [
        ("ZYDUSWELL", date(2023, 12, 14), True),
        ("HINDZINC", date(2023, 12, 29), True),
        ("RELIANCE", date(2023, 12, 14), False),
        ("INFY", date(2023, 12, 29), False),
    ],
)
def test_calculations_price_crossing_above_flag(
    identifier: str, for_date: date, flag: bool
):
    # Prepare calculation pipeline
    pipelines = MultiDataCalculationPipelines()
    pipelines.set_item(
        "vwap",
        CalculationPipelineBuilder.create_pipeline_for_worker(
            VwapCalculationWorker(time_window=50)
        ),
    )
    pipelines.set_item(
        "vwap_crossed",
        CalculationPipelineBuilder.create_pipeline_for_worker(
            PriceCrossedAboveColumnValueFlagWorker(value_column=CalculatedColumns.Vwap)
        ),
    )
    equity_processor.set_calculation_pipelines(pipelines)

    # Run the pipeline and get data
    equity_processor.run_calculation_pipelines()
    daily_data = equity_result.get_daily_data()

    check_col_values(
        data=daily_data.query(
            str(IdentifierFilter(identifier) & DateFilter(for_date=for_date))
        ),
        col_value_pairs={"PriceCrossedAboveVwap": flag},
    )
    assert pipelines.get_calculation_window().trailing == 50



@pytest.mark.parametrize(
    "identifier,N,for_date,value,value_perc",
    [
        ("RELIANCE", 5, date(2023, 12, 14), 7.1, 0.28813181015765715),
        ("INFY", 10, date(2023, 12, 29), 41.45, 2.686499449),
    ],
)
def test_calculations_growth(
    identifier: str, N: int, for_date: date, value: float, value_perc: float
):
    # Prepare calculation pipeline
    pipelines = MultiDataCalculationPipelines()
    pipelines.set_item(
        "growth",
        CalculationPipelineBuilder.create_pipeline_for_worker(
            ColumnGrowthCalculationWorker(N=N)
        ),
    )
    equity_processor.set_calculation_pipelines(pipelines)

    # Run the pipeline and get data
    equity_processor.run_calculation_pipelines()
    daily_data = equity_result.get_daily_data()

    check_col_values(
        data=daily_data.query(
            str(IdentifierFilter(identifier) & DateFilter(for_date=for_date))
        ),
        col_value_pairs={
            f"CloseGrowth{str(N)}Sessions": value,
            f"CloseGrowthPerc{str(N)}Sessions": value_perc
        }
    )

    assert pipelines.get_calculation_window().trailing == N


    
@pytest.mark.parametrize(
    "identifier,for_date,value,value_perc",
    [
        ("RELIANCE", date(2023, 12, 14), 10.15, 0.4119067),
        ("INFY", date(2023, 12, 29), 1.85, 0.11990407),
    ],
)
def test_calculations_columns_delta(
    identifier: str, for_date: date, value: float, value_perc: float
):
    # Prepare calculation pipeline
    pipelines = MultiDataCalculationPipelines()
    pipelines.set_item(
        "columns_delta",
        CalculationPipelineBuilder.create_pipeline_for_worker(
            ColumnsDeltaCalculationWorker(value_column_a=BaseColumns.Close, value_column_b=BaseColumns.Open)
        ),
    )
    equity_processor.set_calculation_pipelines(pipelines)

    # Run the pipeline and get data
    equity_processor.run_calculation_pipelines()
    daily_data = equity_result.get_daily_data()

    check_col_values(
        data=daily_data.query(
            str(IdentifierFilter(identifier) & DateFilter(for_date=for_date))
        ),
        col_value_pairs={
            f"{BaseColumns.Close}-{BaseColumns.Open}": value,
            f"{BaseColumns.Close}-{BaseColumns.Open}AsPerc": value_perc
        }
    )

    assert pipelines.get_calculation_window().trailing == 0


@pytest.mark.parametrize(
    "identifier,for_date,flag",
    [
        ("AETHER", date(2023, 12, 15), True),
        ("VAKRANGEE", date(2023, 12, 29), True),
        ("ZFCVINDIA", date(2023, 12, 28), False)
    ],
)
def test_calculations_col_a_crossed_above_col_b_flag(
    identifier: str, for_date: date, flag: bool
):
    # Prepare calculation pipeline
    pipelines = MultiDataCalculationPipelines()
    pipelines.set_item(
        "cal",
        CalculationPipelineBuilder.create_pipeline_for_worker(
            SmaCalculationWorker(time_window=50)
        ),
    )
    pipelines.set_item(
        "flag",
        CalculationPipelineBuilder.create_pipeline_for_worker(
            ColumnValueCrossedAboveAnotherColumnValueFlagWorker(value_column_a=BaseColumns.Close, value_column_b='Sma50')
        ),
    )
    equity_processor.set_calculation_pipelines(pipelines)

    # Run the pipeline and get data
    equity_processor.run_calculation_pipelines()
    daily_data = equity_result.get_daily_data()

    check_col_values(
        data=daily_data.query(
            str(IdentifierFilter(identifier) & DateFilter(for_date=for_date))
        ),
        col_value_pairs={"CloseCrossedAboveSma50": flag},
    )
    assert pipelines.get_calculation_window().trailing == 50


@pytest.mark.parametrize(
    "identifier,for_date,flag",
    [
        ("AETHER", date(2023, 12, 14), True),
        ("AMBER", date(2023, 12, 28), True),
        ("ASTRAL", date(2023, 12, 14), False)
    ],
)
def test_calculations_col_a_crossed_below_col_b_flag(
    identifier: str, for_date: date, flag: bool
):
    # Prepare calculation pipeline
    pipelines = MultiDataCalculationPipelines()
    pipelines.set_item(
        "cal",
        CalculationPipelineBuilder.create_pipeline_for_worker(
            SmaCalculationWorker(time_window=50)
        ),
    )
    pipelines.set_item(
        "flag",
        CalculationPipelineBuilder.create_pipeline_for_worker(
            ColumnValueCrossedBelowAnotherColumnValueFlagWorker(value_column_a=BaseColumns.Close, value_column_b='Sma50')
        ),
    )
    equity_processor.set_calculation_pipelines(pipelines)

    # Run the pipeline and get data
    equity_processor.run_calculation_pipelines()
    daily_data = equity_result.get_daily_data()

    check_col_values(
        data=daily_data.query(
            str(IdentifierFilter(identifier) & DateFilter(for_date=for_date))
        ),
        col_value_pairs={"CloseCrossedBelowSma50": flag},
    )
    assert pipelines.get_calculation_window().trailing == 50


@pytest.mark.parametrize(
    "identifier,for_date,flag",
    [
        ("3IINFOLTD", date(2023, 12, 14), True),
        ("ZEEL", date(2023, 12, 29), True),
        ("RIIL", date(2023, 12, 14), False)
    ],
)
def test_calculations_col_value_below_flag(
    identifier: str, for_date: date, flag: bool
):
    # Prepare calculation pipeline
    pipelines = MultiDataCalculationPipelines()
    pipelines.set_item(
        "cal",
        CalculationPipelineBuilder.create_pipeline_for_worker(
            SmaCalculationWorker(time_window=50)
        ),
    )
    pipelines.set_item(
        "flag",
        CalculationPipelineBuilder.create_pipeline_for_worker(
            ColumnValueBelowFlagWorker(value_column='Sma50', value=500)
        ),
    )
    equity_processor.set_calculation_pipelines(pipelines)

    # Run the pipeline and get data
    equity_processor.run_calculation_pipelines()
    daily_data = equity_result.get_daily_data()

    check_col_values(
        data=daily_data.query(
            str(IdentifierFilter(identifier) & DateFilter(for_date=for_date))
        ),
        col_value_pairs={"Sma50Below500": flag},
    )
    assert pipelines.get_calculation_window().trailing == 50


@pytest.mark.parametrize(
    "identifier,for_date,flag",
    [
        ("3MINDIA", date(2023, 12, 14), True),
        ("ZENSARTECH", date(2023, 12, 29), True),
        ("MINDACORP", date(2023, 12, 29), False)
    ],
)
def test_calculations_col_value_above_flag(
    identifier: str, for_date: date, flag: bool
):
    # Prepare calculation pipeline
    pipelines = MultiDataCalculationPipelines()
    pipelines.set_item(
        "cal",
        CalculationPipelineBuilder.create_pipeline_for_worker(
            SmaCalculationWorker(time_window=50)
        ),
    )
    pipelines.set_item(
        "flag",
        CalculationPipelineBuilder.create_pipeline_for_worker(
            ColumnValueAboveFlagWorker(value_column='Sma50', value=500)
        ),
    )
    equity_processor.set_calculation_pipelines(pipelines)

    # Run the pipeline and get data
    equity_processor.run_calculation_pipelines()
    daily_data = equity_result.get_daily_data()

    check_col_values(
        data=daily_data.query(
            str(IdentifierFilter(identifier) & DateFilter(for_date=for_date))
        ),
        col_value_pairs={"Sma50Above500": flag},
    )
    assert pipelines.get_calculation_window().trailing == 50


@pytest.mark.parametrize(
    "identifier,for_date,flag",
    [
        ("AARTIDRUGS", date(2023, 12, 14), True),
        ("ZEEMEDIA", date(2023, 12, 29), True),
        ("OLECTRA", date(2023, 12, 29), False)
    ],
)
def test_calculations_col_value_below_another_flag(
    identifier: str, for_date: date, flag: bool
):
    # Prepare calculation pipeline
    pipelines = MultiDataCalculationPipelines()
    pipelines.set_item(
        "cal",
        CalculationPipelineBuilder.create_pipeline_for_worker(
            SmaCalculationWorker(time_window=50)
        ),
    )
    pipelines.set_item(
        "flag",
        CalculationPipelineBuilder.create_pipeline_for_worker(
            ColumnValueBelowAnotherColumnValueFlagWorker(value_column_a=BaseColumns.Close, value_column_b='Sma50')
        ),
    )
    equity_processor.set_calculation_pipelines(pipelines)

    # Run the pipeline and get data
    equity_processor.run_calculation_pipelines()
    daily_data = equity_result.get_daily_data()

    check_col_values(
        data=daily_data.query(
            str(IdentifierFilter(identifier) & DateFilter(for_date=for_date))
        ),
        col_value_pairs={"CloseBelowSma50": flag},
    )
    assert pipelines.get_calculation_window().trailing == 50


@pytest.mark.parametrize(
    "identifier,for_date,flag",
    [
        ("3MINDIA", date(2023, 12, 14), True),
        ("SWARAJENG", date(2023, 12, 29), True),
        ("ZEEMEDIA", date(2023, 12, 29), False)
    ],
)
def test_calculations_col_value_above_another_flag(
    identifier: str, for_date: date, flag: bool
):
    # Prepare calculation pipeline
    pipelines = MultiDataCalculationPipelines()
    pipelines.set_item(
        "cal",
        CalculationPipelineBuilder.create_pipeline_for_worker(
            SmaCalculationWorker(time_window=50)
        ),
    )
    pipelines.set_item(
        "flag",
        CalculationPipelineBuilder.create_pipeline_for_worker(
            ColumnValueAboveAnotherColumnValueFlagWorker(value_column_a=BaseColumns.Close, value_column_b='Sma50')
        ),
    )
    equity_processor.set_calculation_pipelines(pipelines)

    # Run the pipeline and get data
    equity_processor.run_calculation_pipelines()
    daily_data = equity_result.get_daily_data()

    check_col_values(
        data=daily_data.query(
            str(IdentifierFilter(identifier) & DateFilter(for_date=for_date))
        ),
        col_value_pairs={"CloseAboveSma50": flag},
    )
    assert pipelines.get_calculation_window().trailing == 50