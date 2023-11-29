from os import environ

def get_env_var_config() -> dict:
    """
    Returns a dictionary of the connection parameters using the SnowSQL CLI
    environment variables.
    """
    try:
        return {
            "user": environ["SNOWSQL_USER"],
            "password": environ["SNOWSQL_PWD"],
            "account": environ["SNOWSQL_ACCOUNT"],
            # "role": environ["SNOWSQL_ROLE"],
            "warehouse": environ["SNOWSQL_WAREHOUSE"],
            "database": environ["SNOWSQL_DATABASE"],
            "schema": environ["SNOWSQL_SCHEMA"],
        }
    except KeyError as exc:
        raise KeyError(
            "ERROR: Environment variable for Snowflake Connection not found. "
            + "Please set the SNOWSQL_* environment variables"
        ) from exc
