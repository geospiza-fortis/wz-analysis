
$env:SPARK_HOME = $(python -c "import pyspark; print(pyspark.__path__[0])")
$env:PYSPARK_DRIVER_PYTHON = "jupyter"
$env:PYSPARK_DRIVER_PYTHON_OPTS = "notebook"

# https://github.com/databricks/spark-xml
pyspark `
    --master 'local[*]' `
    --conf spark.driver.memory=8g `
    --conf spark.sql.shuffle.partitions=8 `
    --packages com.databricks:spark-xml_2.12:0.10.0
