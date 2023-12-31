# [Local Testing Turorial](https://docs.snowflake.com/en/developer-guide/snowpark/python/tutorials/testing-tutorial)

Looking for the tutorial instructions? [Click here!](https://docs.snowflake.com/en/developer-guide/snowpark/python/tutorials/testing-tutorial)

## Contents

- [`setup/`](setup/): Setup script for the tutorial
- [`project/`](project/): Sample source code for the tutorial
- [`complete.md`](complete.md): Completed tutorial code, use for reference if you get stuck!

## Setup

1. Clone or download this repository
1. Create conda env:

    ```
    conda env create -f environment.yml
    conda activate snowpark-testing
    ```

2. Set your account credentials:

    ```bash
    # Linux/MacOS
    export SNOWSQL_ACCOUNT=<replace with your account identifer>
    export SNOWSQL_USER=<replace with your username>
    export SNOWSQL_ROLE=<replace with your role>
    export SNOWSQL_PWD=<replace with your password>
    export SNOWSQL_DATABASE=<replace with your database>
    export SNOWSQL_SCHEMA=<replace with your schema>
    export SNOWSQL_WAREHOUSE=<replace with your warehouse>
    ```

