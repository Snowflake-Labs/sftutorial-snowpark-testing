from project.utils import get_env_var_config
from snowflake.snowpark.session import Session


def set_up_tables(session: Session):
    DB = 'CITIBIKE'
    SCHEMA = 'PUBLIC'
    TEST_SCHEMA = 'TEST'

    # Create the DB and Schema
    session.sql(F'CREATE DATABASE IF NOT EXISTS {DB}').collect()
    session.sql(F'CREATE SCHEMA IF NOT EXISTS {DB}.{SCHEMA}').collect()
    session.sql(F'CREATE SCHEMA IF NOT EXISTS {DB}.{TEST_SCHEMA}').collect()

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

    tbl.write.mode('overwrite').save_as_table([DB, SCHEMA, 'TRIPS'], mode='overwrite')


def main():
    print('Creating session from environment variables')
    session = Session.builder.configs(get_env_var_config()).create()

    print('Creating tables...')
    set_up_tables(session=session)


if __name__ == '__main__':
    main()
