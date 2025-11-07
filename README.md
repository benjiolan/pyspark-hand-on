# pyspark-hand-on
```bash
# Download Apache Spark:
sudo wget -nc -P /opt/spark/ https://dlcdn.apache.org/spark/spark-3.5.7/spark-3.5.7-bin-hadoop3.tgz

# Verificar y descomprimir en una línea
[ -f "/opt/spark/spark-3.5.7-bin-hadoop3/README.md" ] && echo "Ya descomprimido" || echo "Descomprimiendo.." && sudo tar -xzf /opt/spark/spark-3.5.7-bin-hadoop3.tgz -C /opt/spark/
```
jars:
```
https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-connector-common/0.43.1/spark-bigquery-connector-common-0.43.1-javadoc.jar

```

Spark GCP:
```python
from pyspark.sql import SparkSession
    
JARS_PATH = '/LOCATION-TO-JARS/gcs-connector-hadoop3-latest.jar,/LOCATION-TO-JARS/spark-bigquery-latest_2.12.jar'

spark = sparkSession.builder.appName(SPARK_APP_NAME).config('spark.jars’,JARS_PATH)
.config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GOOGLE_APPLICATION_CREDENTIALS).getOrCreate()
    spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
    spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')
    spark._jsc.hadoopConfiguration().set('fs.gs.project.id', ‘MY-GCP-PROJECT-ID’)
    spark._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    
)
```