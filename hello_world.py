'''
Used counter strike's data set from kaggle:
https://www.kaggle.com/datasets/skihikingkevin/csgo-matchmaking-damage?resource=download
'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

dataset_path = "C:\\Users\\mesam\\Documents\\dataset\\archive\\mm_master_demos.csv"

def read_csv_data(dataset_path,count=None,show=None):
	df = spark.read.csv(dataset_path,header=True,escape="\"")
	
	if count:
		row_count = df.count()
		print("The number of rows is: %s"% (row_count)) 
	elif show:
		print("Showing 5 records")
		df.show(5,0)

	'''
	random sql equivalent queries
	'''
	# select count equivalent
	print("count equivalent query")
	unique_hitbox_count = df.select('hitbox').distinct().show() 	
	
	# groupy equivalent
	print("group by equivalent query")
	df.groupBy('wp_type').agg(countDistinct('map').alias('att_side')).show()

if __name__ == "__main__":
	spark = SparkSession.builder.appName("Pyspark playground") \
	                            .config("spark.memory.offHeap.enabled","true") \
	                            .config("spark.memory.offHeap.size","10g") \
	                            .getOrCreate()
	read_csv_data(dataset_path)
