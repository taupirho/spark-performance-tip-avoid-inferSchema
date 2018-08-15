# Spark Performance Tip – Avoid using the InferSchema option when reading large text files using Spark Sql


Note that for the purposes of this exercise I’m using an input text file consisting of pipe separated fields and containing approximately 6.5 million records. The first ten records of the file are shown below:

```
18511|50|32639952|2016-04-30|0|269375960|0|68.39|26.24091|0|0|1||
18511|50|117903433|2016-03-31|0|2325005|0||15.66319|0|0|4||
18511|50|234816910|2016-06-30|0|3958501|0|27.74|3.3079|-2250000|-36.24|9||1
18511|50|282918722|2016-05-31|0|0|0|0|0|-5795813|-100|||1
18511|50|287515364|2016-04-27|0|1543128|0|2.46|1.90132|-7051262|-82.04|12||1
18511|50|330280518|2016-04-27|0|2107120|0|1.41|2.09247|2107120||20|20|
18515|50|288527113|2016-03-15|0|1987366|0|100|12.9661|0|0|4||
18527|50|20063753|2016-05-04|0|538340870|0||21.16271|0|0|1||
18527|50|20228450|2016-06-28|0|2916387|0||49.85077|0|0|1||
18527|50|34636339|2016-06-30|0|158931281|0||75|0|0|1||
```

When using Spark Sql to load data in from text files like the one above one of the commonest pieces of coding you’ll likely 
see is something like the following:

df = spark.read.format("com.databricks.spark.csv") \
    .option("header", "false").option("inferSchema", "true") \
    .option("delimiter", '|').load("mytextfile.txt") 

df.printSchema()

This results in an output similar to that shown below
root
 |-- _c0: integer (nullable = true)
 |-- _c1: integer (nullable = true)
 |-- _c2: integer (nullable = true)
 |-- _c3: timestamp (nullable = true)
 |-- _c4: integer (nullable = true)
 |-- _c5: long (nullable = true)
 |-- _c6: integer (nullable = true)
 |-- _c7: double (nullable = true)
 |-- _c8: double (nullable = true)
 |-- _c9: long (nullable = true)
 |-- _c10: double (nullable = true)
 |-- _c11: integer (nullable = true)
 |-- _c12: integer (nullable = true)
 |-- _c13: integer (nullable = true)

For relatively small text files this code is absolutely fine and the InferSchema option is particularly useful as it does what it 
says on the tin. When Spark see this directive it tries to derive the most suitable data types for the fields contained in your 
text file and this can be useful and time saving for further transformations you might want to do on your data set. However for 
very large files there is an equally large snag.  And that is that the inferSchema option is used Spark has to check each and every 
line of the whole file before it can derive the final schema. For files that contain millions or even billions of records that is 
a serious performance issue. To give you an idea of type of performance hit you can expect to receive I wrote the following little 
bit of pyspark code in a Jupyter notebook to read in and count the records in my 6.5 million record test file.


import findspark
findspark.init()
import pyspark
import datetime

sc = pyspark.SparkContext(appName="read-big-file")

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .getOrCreate()

print(datetime.datetime.now())

df = spark.read.format("csv") \
    .option("header", "false").option("inferSchema", "true") \
    .option("delimiter", '|').load("file:///d:/tmp/iholding/myfiles/issue50.txt")

print(“Nr of records = “,df.count())

print(datetime.datetime.now())

I ran the code twice but on the second running I removed the .option("inferSchema", "true") line. The results are shown below

Run 1 with the inferSchema option

2018-08-15 12:29:34.294535
Nr of records =  6512888
2018-08-15 12:29:58.310798

Run 2 without the inferSchema option

2018-08-15 12:30:09.862194
Nr of records =  6512888
2018-08-15 12:30:12.956043

Using inferSchema imposed a 21 second overhead on the running of this code and remember that the size of the input file in big data terms was still relatively modest.
So, if inferSchema is a useful option when reading text files how do we get around this problem when dealing with bigger data files. There are two main ways.
The first way involves using the built-in pyspark sql datatypes such as  StructType and StructFields   to construct your own schema that corresponds to the file layout. For our example we would use something like this:

from pyspark.sql.types import StructField, StructType, DoubleType, LongType,\
IntegerType, TimestampType

myschema = StructType([
        StructField("column1", IntegerType(), True),
        StructField("column2", IntegerType(), True),
        StructField("column3", IntegerType(), True),
        StructField("column4", TimestampType(), True),
        StructField("column5", IntegerType(), True),
        StructField("column6", LongType(), True),
        StructField("column7", IntegerType(), True),
        StructField("column8", DoubleType(), True),
        StructField("column9", DoubleType(), True),
        StructField("column10",LongType(), True),
        StructField("column11", DoubleType(), True),
        StructField("column12", IntegerType(), True),
        StructField("column13", IntegerType(), True)
            ])

In our read statement, instead of using inferSchema we use the .schema directive like this:

df = spark.read.format("com.databricks.spark.csv") \
    .option("header", "false").schema(myschema)\
    .option("delimiter", '|').load("file:///d:/tmp/iholding/myfiles/issue50.txt")

A second way which may be easier especially if you have files that contain a large number of columns is to create a little 
one line test file containing an accurate representation of the fields in your main file(s). Then you can simply infer the schema 
from the test file and apply it to your main dat file. Here’s an example.

dummyDf = spark.read.format("csv") \
    .option("header", "false").option(“inferSchema”,”true”) \
    .option("delimiter", '|').load("file:///d:/tmp/iholding/myfiles/tinyfile.txt")

df = spark.read.format("csv") \
    .option("header", "false").schema(dummyDf.schema)\
    .option("delimiter", '|').load("file:///d:/tmp/iholding/myfiles/issue50.txt")
    

NB The above will work for all text delimited filesd as well as JSOn format and splittable compressed versions of them too. 
