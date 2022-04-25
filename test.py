from pyspark.sql import SparkSession
from stitch import loading_stitch_chemical_sources
from sider import loading_sider_toxicity_cid, loading_sider_indications_cid
import re

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

a = loading_stitch_chemical_sources()
b = loading_sider_toxicity_cid()
df_stitch = spark.createDataFrame(a, schema='cid_ok string, ac string')
df_sider = spark.createDataFrame(b, schema='cid string, sy string').distinct()

sympt = "Prurigo"

df = df_sider
acc = df.filter(df.sy.contains('Sinus '))

