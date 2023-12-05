# Compeleted tutorial

Got stuck in the tutorial? Here are the completed files from the `test/` directory:

## conftest.py

```python
import pytest
from project.utils import get_env_var_config
from snowflake.snowpark.session import Session

def pytest_addoption(parser):
    parser.addoption("--snowflake-session", action="store", default="live")

@pytest.fixture(scope='module')
def session(request) -> Session:
    if request.config.getoption('--snowflake-session') == 'local':
        return Session.builder.config('local_testing', True).create()
    else:
        return Session.builder.configs(get_env_var_config()).create()
```

## patches.py

```python
from snowflake.snowpark.mock.functions import patch
from snowflake.snowpark.functions import monthname
from snowflake.snowpark.mock.snowflake_data_type import ColumnEmulator, ColumnType
from snowflake.snowpark.types import StringType
import datetime
import calendar

@patch(monthname)
def patch_monthname(column: ColumnEmulator) -> ColumnEmulator:
    ret_column = ColumnEmulator(data=[
        calendar.month_abbr[datetime.datetime.strptime(row, '%Y-%m-%d %H:%M:%S.%f %z').month]
        for row in column])
    ret_column.sf_type = ColumnType(StringType(), True)
    return ret_column
```

## test_transformers.py

```python
from project.transformers import add_rider_age, calc_bike_facts, calc_month_facts
from snowflake.snowpark.types import StructType, StructField, IntegerType, FloatType

def test_add_rider_age(session):
    input = session.create_dataframe(
        [
            [1980], 
            [1995], 
            [2000]
        ], 
        schema=StructType([StructField("BIRTH_YEAR", IntegerType())])
    )

    expected = session.create_dataframe(
        [
            [1980, 43], 
            [1995, 28], 
            [2000, 23]
        ],
        schema=StructType([StructField("BIRTH_YEAR", IntegerType()), StructField("RIDER_AGE", IntegerType())])
    )
    
    actual = add_rider_age(input)
    assert expected.collect() == actual.collect()


def test_calc_bike_facts(session):
    input = session.create_dataframe([
            [1, 10, 20],
            [1, 5, 30],
            [2, 20, 50],
            [2, 10, 60]
        ], 
        schema=StructType([
            StructField("BIKEID", IntegerType()), 
            StructField("TRIPDURATION", IntegerType()), 
            StructField("RIDER_AGE", IntegerType())
        ])
    )

    expected = session.create_dataframe([
            [1, 2, 7.5, 25.0],
            [2, 2, 15.0, 55.0],
        ], 
        schema=StructType([
            StructField("BIKEID", IntegerType()), 
            StructField("COUNT", IntegerType()), 
            StructField("AVG_TRIPDURATION", FloatType()), 
            StructField("AVG_RIDER_AGE", FloatType())
        ])
    )

    actual = calc_bike_facts(input)
    assert expected.collect() == actual.collect()


def test_calc_month_facts(request, session):
    if request.config.getoption('--snowflake-session') == 'local':
        from patches import patch_monthname

    input = session.create_dataframe(
        data=[
            ['2018-03-01 09:47:00.000 +0000', 1, 10,  15],
            ['2018-03-01 09:47:14.000 +0000', 2, 20, 12],
            ['2018-04-01 09:47:04.000 +0000', 3, 6,  30]
        ],
        schema=['STARTTIME', 'BIKE_ID', 'TRIPDURATION', 'RIDER_AGE']
    )

    expected = session.create_dataframe(
        data=[
            ['Mar', 2, 15, 13.5],
            ['Apr', 1, 6, 30.0]
        ],
        schema=['MONTH', 'COUNT', 'AVG_TRIPDURATION', 'AVG_RIDER_AGE']
    )

    actual = calc_month_facts(input)

    assert expected.collect() == actual.collect()
```

## test_sproc.py

```python
from project.sproc import create_fact_tables
from snowflake.snowpark.types import *


def test_create_fact_tables(request, session):
    if request.config.getoption('--snowflake-session') == 'local':
        from patches import patch_monthname
        
    DB = 'CITIBIKE'
    SCHEMA = 'TEST'
    TABLE = 'TRIPS_TEST'

    # Set up source table
    tbl = session.create_dataframe(
        data=[
            [1983, '2018-03-01 09:47:00.000 +0000', 551, 30958],
            [1988, '2018-03-01 09:47:01.000 +0000', 242, 19278],
            [1992, '2018-03-01 09:47:01.000 +0000', 768, 18461],
            [1980, '2018-03-01 09:47:03.000 +0000', 690, 15533],
            [1991, '2018-03-01 09:47:03.000 +0000', 490, 32449],
            [1959, '2018-03-01 09:47:04.000 +0000', 457, 29411],
            [1971, '2018-03-01 09:47:08.000 +0000', 279, 28015],
            [1964, '2018-03-01 09:47:09.000 +0000', 546, 15148],
            [1983, '2018-03-01 09:47:11.000 +0000', 358, 16967],
            [1985, '2018-03-01 09:47:12.000 +0000', 848, 20644],
            [1984, '2018-03-01 09:47:14.000 +0000', 295, 16365]
        ],
        schema=['BIRTH_YEAR', 'STARTTIME', 'TRIPDURATION',	'BIKEID'],
    )

    tbl.write.mode('overwrite').save_as_table([DB, SCHEMA, TABLE], mode='overwrite')

    # Expected values
    n_rows_expected = 12 
    bike_facts_expected = session.create_dataframe(
        data=[
            [30958, 1, 551.0, 40.0], 
            [19278, 1, 242.0, 35.0], 
            [18461, 1, 768.0, 31.0],
            [15533, 1, 690.0, 43.0], 
            [32449, 1, 490.0, 32.0], 
            [29411, 1, 457.0, 64.0], 
            [28015, 1, 279.0, 52.0], 
            [15148, 1, 546.0, 59.0], 
            [16967, 1, 358.0, 40.0], 
            [20644, 1, 848.0, 38.0], 
            [16365, 1, 295.0, 39.0]
        ],
        schema=StructType([
            StructField("BIKEID", IntegerType()), 
            StructField("COUNT", IntegerType()), 
            StructField("AVG_TRIPDURATION", FloatType()), 
            StructField("AVG_RIDER_AGE", FloatType())
        ])
    ).collect()

    month_facts_expected = session.create_dataframe(
        data=[['Mar', 11, 502.18182, 43.00000]],
        schema=StructType([
            StructField("MONTH", StringType()), 
            StructField("COUNT", IntegerType()), 
            StructField("AVG_TRIPDURATION", DecimalType()), 
            StructField("AVG_RIDER_AGE", DecimalType())
        ])
    ).collect()

    # Call sproc, get actual values
    n_rows_actual = create_fact_tables(session, SCHEMA, TABLE)
    bike_facts_actual = session.table([DB, SCHEMA, 'bike_facts']).collect() 
    month_facts_actual = session.table([DB, SCHEMA, 'month_facts']).collect() 

    # Comparisons
    assert n_rows_expected == n_rows_actual
    assert bike_facts_expected == bike_facts_actual
    #assert month_facts_expected ==  month_facts_actual
```
