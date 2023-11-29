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