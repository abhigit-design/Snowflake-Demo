COPY INTO sample_sales
FROM @~/sample_sales_data.csv
FILE_FORMAT = (FORMAT_NAME = my_csv_format)
ON_ERROR = 'CONTINUE';