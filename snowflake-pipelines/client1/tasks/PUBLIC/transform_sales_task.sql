create or replace task TRANSFORM_SALES_TASK
	warehouse=COMPUTE_WH
	schedule='USING CRON * * * * * UTC'
	when SYSTEM$STREAM_HAS_DATA('sample_sales_stream')
	as INSERT INTO sample_sales_clean (PRODUCT, QUANTITY, ORDER_DATE)
SELECT
    TRIM(PRODUCT) AS PRODUCT,
    CAST(QUANTITY AS NUMBER(10,2)) AS QUANTITY,
    ORDER_DATE
FROM sample_sales_stream
WHERE METADATA$ACTION = 'INSERT';