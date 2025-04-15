import requests
import snowflake.connector as sf
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd

result=requests.get('https://fakestoreapi.com/products')
data_json = result.json()


id = []
title = []
price = []
descriprion = []
category = []
image = []
rate = []
count = []

for product in data_json:
    id.append(product["id"])
    title.append(product["title"])
    price.append(product["price"])
    descriprion.append(product["description"])
    category.append(product["category"])
    image.append(product["image"])
    rate.append(product["rating"]["rate"])
    count.append(product["rating"]["count"])

product_dict = {
    "ID": id,
    "Title": title,
    "Price": price,
    "Description": descriprion,
    "Category": category,
    "Image": image,
    "Rate": rate,
    "Count": count
}

products_df = pd.DataFrame(product_dict)

print(products_df.head(5))


conn = sf.connect(
    user="CHIRAGDUDHREJIA",
    password="X4bjQ8qYYKN54kB",
    account="BQQJQOL-QTB09420",
    warehouse="COMPUTE_WH"
    )


try:
    sf_cursor_obj = conn.cursor()
    sf_cursor_obj.execute("USE DATABASE CONNECTOR_PRACTICE")
    sf_cursor_obj.execute("CREATE SCHEMA IF NOT EXISTS FAKE_STORE_DATA")
    sf_cursor_obj.execute("USE SCHEMA FAKE_STORE_DATA")

    create_table_product_sql = "CREATE TABLE IF NOT EXISTS PRODUCT(" \
    "ID INT," \
    "TITLE VARCHAR(200)," \
    "PRICE NUMERIC(7, 2)," \
    "DESCRIPTION VARCHAR(1000)," \
    "CATEGORY VARCHAR(200)," \
    "IMAGE VARCHAR(100)," \
    "RATE NUMERIC(3, 1)," \
    "COUNT INT" \
    ")"
    sf_cursor_obj.execute(create_table_product_sql)

    success, nchunks, nrows, _ = write_pandas(conn, products_df, 'PRODUCT', quote_identifiers=False)

    print(f"{success}, {nchunks}, {nrows}")
except Exception as e:
    print(e)
finally:
    sf_cursor_obj.close()

conn.close()

