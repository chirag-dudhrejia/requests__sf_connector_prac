import toml
import snowflake.connector as sf
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import requests
import json
import traceback
import toml

connections_file = toml.load('connections.toml')
conn_params = connections_file['fakestoredata']

response = requests.get(url="https://fakestoreapi.com/users")

res_json = response.json()

user_data_df = pd.DataFrame(res_json)


print(user_data_df)

config_file = toml.load("connections.toml")
conn_params = config_file["fakestoredata"]

conn = sf.connect(
    user=conn_params['user'],
    password=conn_params['password'],
    account=conn_params['account'],
    warehouse=conn_params['warehouse'],
    role=conn_params['role'],
    database=conn_params['database'],
    schema=conn_params['schema']
    )


try:
    sf_cursor_obj = conn.cursor()

    # sf_cursor_obj.execute("CREATE TABLE IF NOT EXISTS USER_RAW(u VARIANT)")


    file_path = "user_data.json"
    stage_name = "@CONNECTOR_PRACTICE.FAKE_STORE_DATA.FAKE_STORE"
    with open(file_path, "w") as output_file:
        json.dump(res_json, output_file)

    sf_cursor_obj.execute("CREATE STAGE IF NOT EXISTS FAKE_STORE DIRECTORY = ( ENABLE = true )")

    put_response = sf_cursor_obj.execute(f"PUT file://{file_path} {stage_name} AUTO_COMPRESS = False")

    print(put_response)

except Exception as e:
    traceback.print_exc()
finally:
    sf_cursor_obj.close()

conn.close()