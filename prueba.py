# -*- coding: utf-8 -*-
###
 # @section Import
 ###
import json
import traceback
import codecs
import sys
import subprocess
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark import StorageLevel
from pyspark.sql.functions import col, lit, sum, broadcast
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark import SparkConf

###
 # @section configuracion de recursos
 ##

spark = SparkSession.builder.\
appName("PROCESO").\
config("spark.driver.cores", "1").\
config("spark.executor.cores", "4").\
config("spark.executor.memory", "22g").\
config("spark.driver.memory", "1g").\
config("spark.dynamicAllocation.maxExecutors", "20").\
config("spark.executor.memoryOverhead", "4g").\
config("spark.driver.memoryOverhead", "1g").\
enableHiveSupport().\
master("yarn").\
getOrCreate()

spark.conf.set("spark.shuffle.service.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "1000")
spark.conf.set("spark.default.parallelism", "1000")
spark.conf.set("spark.debug.maxToStringField", "1000")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "1048576000")
spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.ui.enabled", "true")
spark.conf.set("spark.yarn.queue", "default")

###
 # @section Parametros de proceso
 ##


###
 # @Section Proceso
 ##
 
def runProcessPartition():
	partitionDf = spark.sql(
	"""
	SELECT 
	codclavecta , 
	codclavectaagrupado , 
	codclaveproducto , 
    mtocompromisopago
	FROM BCP_UDV_INT.M_CUENTACOMERCIOEXTERIOR_BKP_PBM
	""")
	

	print("Se ingesto la tabla M_CUENTACOMERCIOEXTERIOR")	

###
 # Metodo principal
 # @return {void}
 ##
def main():
	runProcessPartition()
	

##Inicio Proceso main()
main()

##Liberar recursos
spark.stop()

##Salir
exit()
