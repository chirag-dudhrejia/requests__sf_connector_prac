import queries

def stage_to_raw(sf_cursor):
    result = sf_cursor.execute(queries.copy_into_user_raw_from_stage)
    output = result.fetchall()

    print(len(output))
    print(output)

    if "LOADED" in output[0]:
        return 1
    else:
        return 0