from datetime import date, timedelta
import pytest
import pandas as pd

from helper import (
    Presets,
    check_col_values,
    setup,
    check_cols_present,
    check_base_cols_present,
    PresetDates,
)

setup()

from markets_insights.datareader.data_reader import (
    DataReader,
    DateRangeCriteria,
    ForDateCriteria,
    MemoryCachedDataReader,
    NseIndicesReader,
    Status,
)

test_cases = [
    {
        "test_id": 0,
        "data_availability": [
            DateRangeCriteria(date(2019, 1, 1), date(2023, 12, 31))
        ],
        "read_criteria": DateRangeCriteria(date(2020, 1, 1), date(2022, 12, 31)),
        "expected_status": Status.COMPLETE,
        "expected_available": [DateRangeCriteria(date(2020, 1, 1), date(2022, 12, 30))],
        "expected_unavailable": [],
    },
    {
        "test_id": 1,
        "data_availability": [
            DateRangeCriteria(date(2019, 1, 1), date(2019, 12, 31)),
            DateRangeCriteria(date(2021, 1, 1), date(2021, 12, 31)),
        ],
        "read_criteria": DateRangeCriteria(date(2019, 1, 1), date(2021, 12, 31)),
        "expected_status": Status.PARTIAL,
        "expected_available": [
            DateRangeCriteria(date(2019, 1, 1), date(2019, 12, 31)),
            DateRangeCriteria(date(2021, 1, 1), date(2021, 12, 31)),
        ],
        "expected_unavailable": [DateRangeCriteria(date(2020, 1, 1), date(2020, 12, 31))],
    },
    # No Availability
    {
        "test_id": 2,
        "data_availability": [
            DateRangeCriteria(date(2018, 1, 1), date(2018, 12, 31)),
        ],
        "read_criteria": DateRangeCriteria(date(2019, 1, 1), date(2019, 12, 31)),
        "expected_status": Status.NONE,
        "expected_available": [],
        "expected_unavailable": [DateRangeCriteria(date(2019, 1, 1), date(2019, 12, 31))],
    },
    # Exact Match
    {
        "test_id": 3,
        "data_availability": [
            DateRangeCriteria(date(2019, 1, 1), date(2019, 12, 31)),
        ],
        "read_criteria": DateRangeCriteria(date(2019, 1, 1), date(2019, 12, 31)),
        "expected_status": Status.COMPLETE,
        "expected_available": [DateRangeCriteria(date(2019, 1, 1), date(2019, 12, 31))],
        "expected_unavailable": [],
    },
    # Inside Available Range
    {
        "test_id": 4,
        "data_availability": [
            DateRangeCriteria(date(2019, 1, 1), date(2019, 12, 31)),
        ],
        "read_criteria": DateRangeCriteria(date(2019, 3, 1), date(2019, 10, 31)),
        "expected_status": Status.COMPLETE,
        "expected_available": [DateRangeCriteria(date(2019, 3, 1), date(2019, 10, 31))],
        "expected_unavailable": [],
    },
    # Exceeding Available Range
    {
        "test_id": 5,
        "data_availability": [
            DateRangeCriteria(date(2019, 6, 1), date(2019, 7, 31)),
        ],
        "read_criteria": DateRangeCriteria(date(2019, 1, 1), date(2019, 12, 31)),
        "expected_status": Status.PARTIAL,
        "expected_available": [DateRangeCriteria(date(2019, 6, 1), date(2019, 7, 31))],
        "expected_unavailable": [
            DateRangeCriteria(date(2019, 1, 1), date(2019, 5, 31)),
            DateRangeCriteria(date(2019, 8, 1), date(2019, 12, 31)),
        ],
    },
    # Edge Case - Start on Available Range Start
    {
        "test_id": 6,
        "data_availability": [
            DateRangeCriteria(date(2019, 5, 1), date(2019, 7, 31)),
        ],
        "read_criteria": DateRangeCriteria(date(2019, 5, 1), date(2019, 6, 15)),
        "expected_status": Status.COMPLETE,
        "expected_available": [DateRangeCriteria(date(2019, 5, 2), date(2019, 6, 14))],
        "expected_unavailable": [],
    },
    # Edge Case - End on Available Range End
    {
        "test_id": 7,
        "data_availability": [
            DateRangeCriteria(date(2019, 5, 1), date(2019, 7, 31)),
        ],
        "read_criteria": DateRangeCriteria(date(2019, 7, 15), date(2019, 7, 31)),
        "expected_status": Status.COMPLETE,
        "expected_available": [DateRangeCriteria(date(2019, 7, 15), date(2019, 7, 31))],
        "expected_unavailable": [],
    },
    # ForDateRange available
    {
        "test_id": 8,
        "data_availability": [
            DateRangeCriteria(date(2019, 5, 1), date(2019, 7, 31)),
        ],
        "read_criteria": ForDateCriteria(date(2019, 7, 15)),
        "expected_status": Status.COMPLETE,
        "expected_available": [DateRangeCriteria(date(2019, 7, 15), date(2019, 7, 15))],
        "expected_unavailable": None,
    },
    # ForDateRange unavailable
    {
        "test_id": 9,
        "data_availability": [
            DateRangeCriteria(date(2019, 5, 1), date(2019, 7, 31)),
        ],
        "read_criteria": ForDateCriteria(date(2019, 8, 15)),
        "expected_status": Status.NONE,
        "expected_available": None,
        "expected_unavailable": None,
    },
    # Merge Intervals
    {
        "test_id": 10,
        "data_availability": [
            DateRangeCriteria(date(2019, 1, 1), date(2019, 12, 29)),
            DateRangeCriteria(date(2020, 1, 2), date(2020, 7, 31)),
        ],
        "read_criteria": DateRangeCriteria(date(2019, 1, 1), date(2020, 7, 31)),
        "expected_status": Status.COMPLETE,
        "expected_available": [DateRangeCriteria(date(2019, 1, 1), date(2020, 7, 31))],
        "expected_unavailable": [],
    },
]

@pytest.mark.parametrize(
    "test_case",
    test_cases,
)
def test_has_data_for_range(test_case: dict[str, any]):
    reader = DataReader()
    reader.options.data_availability = test_case["data_availability"]

    status = reader.has_data(test_case["read_criteria"])
    assert status.status == test_case["expected_status"]
    assert status.availability_ranges == test_case["expected_available"]
    assert status.unavailability_ranges == test_case["expected_unavailable"]


test_cases2 = [
    {
        "id": 0,
        "read_criterias": [DateRangeCriteria(date(2019, 1, 1), date(2019, 1, 31))],
        "availability_ranges": [DateRangeCriteria(date(2019, 1, 1), date(2019, 1, 31))]
    },
    {
        "id": 1,
        "read_criterias": [
            DateRangeCriteria(date(2019, 1, 1), date(2019, 1, 31)), 
            DateRangeCriteria(date(2019, 2, 1), date(2019, 2, 15))
        ],
        "availability_ranges": [DateRangeCriteria(date(2019, 1, 1), date(2019, 2, 15))]
    },
    {
        "id": 2,
        "read_criterias": [
            DateRangeCriteria(date(2019, 1, 1), date(2019, 1, 31)), 
            DateRangeCriteria(date(2019, 3, 1), date(2019, 3, 15))
        ],
        "availability_ranges": [
            DateRangeCriteria(date(2019, 1, 1), date(2019, 1, 31)), 
            DateRangeCriteria(date(2019, 3, 1), date(2019, 3, 15))
        ]
    },
    {
        "id": 2,
        "read_criterias": [
            DateRangeCriteria(date(2019, 3, 1), date(2019, 3, 15)),
            DateRangeCriteria(date(2019, 1, 1), date(2019, 3, 1))
        ],
        "availability_ranges": [
            DateRangeCriteria(date(2019, 1, 1), date(2019, 3, 15))
        ]
    },
]
@pytest.mark.parametrize(
    "test_case",
    test_cases2,
)
def test_has_data_cached_for_continous_range(test_case):
    reader = MemoryCachedDataReader(NseIndicesReader())

    for read_criteria in test_case["read_criterias"]:
        data: pd.DataFrame = reader.read(read_criteria)
    
    assert reader.options.data_availability == test_case["availability_ranges"]