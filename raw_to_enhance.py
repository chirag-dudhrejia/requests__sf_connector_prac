import queries

def raw_to_enhance_func(sf_cursor):
    show_table_cursor = sf_cursor.execute("SHOW TABLES LIKE 'USER_ENHANCED'")
    table_exists = show_table_cursor.fetchall()

    if len(table_exists) > 0:
        merge_new_data_cursor = sf_cursor.execute(queries.merge_new_data_into_enhanced)
        data_merged = merge_new_data_cursor.fetchall()

        return len(data_merged)
    else:
        create_table_and_load_data_cursor = sf_cursor.execute(queries.create_table_enhanced_and_load_data)
        table_created = create_table_and_load_data_cursor.fetchall()

        return len(table_created)