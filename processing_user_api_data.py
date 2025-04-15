import snowflake.connector as sf
import toml
import traceback
import queries
from stage_to_raw import stage_to_raw
from raw_to_enhance import raw_to_enhance_func

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


if __name__ == "__main__":
    try:
        sf_cursor_obj = conn.cursor()

        raw_data_load = stage_to_raw(sf_cursor_obj)

        enhanced_data_loaded = None
        if raw_data_load:
            enhanced_data_loaded = raw_to_enhance_func(sf_cursor_obj)

        print(enhanced_data_loaded)

        # raw_to_enhance_func(sf_cursor_obj)

        # result = sf_cursor_obj.execute("SHOW TABLES LIKE 'USER_ENHANCED' IN ACCOUNT")
        # output = result.fetchall()

        # if len(output) > 0:
        #     print("Table Exists")
        # else:
        #     sf_cursor_obj.execute(queries.copy_into_user_raw_from_stage)
    except Exception as e:
        traceback.print_exc()
    finally:
        sf_cursor_obj.close()

conn.close()