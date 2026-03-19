# de-zc-finalproject
RealTime Data analysis

Command Run
> uv init --python=3.13
> uv add --dev jupyter
> uv add pandas

Run jupyter notebook
> uv run jupyter notebook

jupyter notebook can be reached in below url
http://localhost:8888/tree?token=c3efa89deefb67ebde3190669adb6f024d1dda4c4a7636c9
        
This url can be used to set jupyter kernel in VS Code, so that we can run notebook from within VS Code.
> touch notebook.ipynb

Tested the notebook.ipynb cell by trying to execute print(123) python code.
This will ask to setup jupyter kernel.

> touch docker-compose.yaml

Added DE ZoomCamp's Kestra docker-compose code 
> docker-compose up -d


Followed linux.md instruction to install Apache Spark in Google VM

> uv add pyspark

Ran a test code in notebook cell to test pyspark is installed successfully

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

print(f"Spark version: {spark.version}")

df = spark.range(10)
df.show()

spark.stop()

Output:
WARNING: Using incubator modules: jdk.incubator.vector
Using Spark's default log4j profile: org/apache/spark/log4j2-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
26/03/19 22:33:22 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Spark version: 4.1.1
                                                                                
+---+
| id|
+---+
|  0|
|  1|
|  2|
|  3|
|  4|
|  5|
|  6|
|  7|
|  8|
|  9|
+---+