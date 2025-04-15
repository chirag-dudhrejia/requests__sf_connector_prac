copy_into_user_raw_from_stage = """COPY INTO CONNECTOR_PRACTICE.FAKE_STORE_DATA.USER_RAW 
FROM '@CONNECTOR_PRACTICE.FAKE_STORE_DATA.FAKE_STORE/' 
file_format = (format_name = json_ff)"""

create_table_enhanced_and_load_data = """CREATE TABLE IF NOT EXISTS CONNECTOR_PRACTICE.FAKE_STORE_DATA.USER_ENHANCED AS (SELECT U:id::INT as ID,
       U:name:firstname::VARCHAR as FirstName,
       U:name:lastname::VARCHAR as LastName,
       U:username::VARCHAR as UserName,
       U:password::VARCHAR as Password,
       U:email::VARCHAR as email,
       U:phone::VARCHAR as Phone,
       U:address:street::VARCHAR as Street,
       U:address:city::VARCHAR as city
FROM CONNECTOR_PRACTICE.FAKE_STORE_DATA.USER_RAW);"""

merge_new_data_into_enhanced = """MERGE INTO CONNECTOR_PRACTICE.FAKE_STORE_DATA.USER_ENHANCED e
USING(
SELECT U:id::INT as ID,
       U:name:firstname::VARCHAR as FirstName,
       U:name:lastname::VARCHAR as LastName,
       U:username::VARCHAR as UserName,
       U:password::VARCHAR as Password,
       U:email::VARCHAR as email,
       U:phone::VARCHAR as Phone,
       U:address:street::VARCHAR as Street,
       U:address:city::VARCHAR as city
FROM CONNECTOR_PRACTICE.FAKE_STORE_DATA.USER_RAW
) r
ON r.UserName=e.username
AND r.email=e.email
WHEN NOT MATCHED THEN
insert(ID, FirstName, LastName, UserName, Password, email, Phone, Street, city)
values(ID, FirstName, LastName, UserName, Password, email, Phone, Street, city);"""