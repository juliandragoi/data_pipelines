import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import time
from pyspark.sql.functions import *
from pyspark.sql.types import DateType
from sqlalchemy import create_engine





# create a SparkSession instance with the name moviedb with Hive support enabled
# https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html
spark = SparkSession.builder.appName("abcd").getOrCreate()

# create a SparkContext instance which allows the Spark Application to access
# Spark Cluster with the help of a resource manager which is usually YARN or Mesos
sc = SparkContext.getOrCreate()

# create a SQLContext instance to access the SQL query engine built on top of Spark
sqlContext = SQLContext(spark)

start_time = time.time()


file = '/Users/juliandragoi/data_pipelines/companies_house/companies_uk.csv'

# df = spark.read.csv(file)

df = spark.read.csv(file, inferSchema=True, header=False)

df.show()
print("--- %s seconds ---" % (time.time() - start_time))

# drop_columns = ['_c2', '_c3', '_c4', '_c5', '_c6', '_c7', '_c10', '_c12', '_c13', '_c15', '_c16', '_c17', '_c18', '_c19'
#     , '_c20', '_c21', '_c22', '_c23', '_c24', '_c25', '_c30', '_c31', '_c32', '_c33', '_c34', '_c35', '_c36', '_c37'
#     , '_c38', '_c39', '_c40', '_c41', '_c42', '_c43', '_c44', '_c45', '_c46', '_c47', '_c48', '_c49', '_c50', '_c50'
#     , '_c51', '_c52', '_c53', '_c54']
#
#
# for i in drop_columns:
#
#     df.drop(i)


# df.select("CompanyName", "RegAddress.PostCode",'RegAddress.PostTown','RegAddress.Country',"CompanyStatus"
#                , "IncorporationDate","SICCode.SicText_1","SICCode.SicText_2","SICCode.SicText_3","SICCode.SicText_4").collect()


main_df = df.select('_c1', '_c2', '_c7', '_c9', '_c10', '_c12', '_c15', '_c27', '_c28','_c29','_c30').filter(df._c12 == "Active")

# main_df = main_df.withColumnRenamed('_c1', 'CompanyName')
# main_df = main_df.withColumnRenamed('_c2', 'CompanyNumber')
# main_df = main_df.withColumnRenamed('_c7', 'RegAddress_PostTown')
# main_df = main_df.withColumnRenamed('_c9', 'RegAddress_Country')
# main_df = main_df.withColumnRenamed('_c10', 'RegAddress_PostCode')
# main_df = main_df.withColumnRenamed('_c12', 'CompanyStatus')
# main_df = main_df.withColumnRenamed('_c15', 'IncorporationDate')
# main_df = main_df.withColumnRenamed('_c27', 'SICCode_SicText_1')
# main_df = main_df.withColumnRenamed('_c28', 'SICCode_SicText_2')
# main_df = main_df.withColumnRenamed('_c29', 'SICCode_SicText_3')
# main_df = main_df.withColumnRenamed('_c30', 'SICCode_SicText_4')

# main_df = main_df.withColumn("IncorporationDate_new",main_df['IncorporationDate'].cast(DateType()))

main_df.createOrReplaceTempView('active_companies')

results = spark.sql("SELECT * FROM active_companies WHERE _c15 LIKE '%2020%' ORDER BY _c15 DESC ")

results.show()
# results.printSchema()

# res_pd = main_df.toPandas()
#
# def get_engine():
#     engine = create_engine('postgresql://pi_user:angrysugar91@192.168.1.204:5432/pi4_db', convert_unicode=True)
#     return engine
#
# res_pd.to_sql(schema='core', name='active_compnaies', con=get_engine(), if_exists='replace')

#
results.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").mode('overwrite').save("active_companies.csv")



print("--- %s seconds ---" % (time.time() - start_time))



# print("--- %s seconds ---" % (time.time() - start_time))
#
# print("Active companies in the UK: " + str(main_df.count()))
#
# print("--- %s seconds ---" % (time.time() - start_time))
#
# sorted_df = main_df.sort(desc("_c14"))
#
# print("--- %s seconds ---" % (time.time() - start_time))
#
# sorted_df.show()

# active_only = df.filter(df._c11 == "Active")
#
# active_only.show()

# df.filter(df._c13 == "Active").count()
# df.show()

# df.createGlobalTempView("companies")
# spark.sql("SELECT RegAddress.PostCode FROM global_temp.companies").show()

# "CompanyName", "RegAddress.PostCode", "CompanyStatus", "IncorporationDate", "SICCode.SicText_1",
# "SICCode.SicText_2", "SICCode.SicText_3", "SICCode.SicText_4"

