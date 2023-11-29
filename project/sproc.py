from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.session import Session
from project.transformers import *
from project.utils import get_env_var_config


def create_fact_tables(sess: Session, schema, source_table) -> int:
    """
    This job applies the transformations in transformers.py to the Citibike dataset
    and creates two tables, month_summary and bike_summary, under CITIBIKE.PUBLIC. 
    The job's return value is the total number of rows created in both tables
    """

    SOURCE_DB = 'CITIBIKE'

    df: DataFrame = sess.table([SOURCE_DB, schema, source_table])
    
    df2 = add_rider_age(df)
    month_facts = calc_month_facts(df2)
    bike_facts = calc_bike_facts(df2)

    month_facts.write.save_as_table([SOURCE_DB, schema, 'MONTH_FACTS'], table_type='', mode='overwrite')
    bike_facts.write.save_as_table([SOURCE_DB, schema, 'BIKE_FACTS'], table_type='', mode='overwrite')

    return month_facts.count() + bike_facts.count()


if __name__ == '__main__':
    print('Creating session')
    session = Session.builder.configs(get_env_var_config()).create()

    print('Running job...')
    rows = create_fact_tables(session, 'PUBLIC', 'TRIPS')

    print(f'Job complete. Number of rows created: {rows}')
