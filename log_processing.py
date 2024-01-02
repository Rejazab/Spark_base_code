path = './work'

'''
Init Spark Session 
'''

import pyspark
import re
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.master('local[1]')\
						.appName('Extract_log_file')\
						.getOrCreate()

'''
Functions to run the checks
'''

indexes = lambda data_list, element: [index for index, string in enumerate(data_list) if element in string]

def getFileList(path):
	'''
	Print the files from the folder in a string format

	Arguments:
	path -- the path to the folder with the txt files

	Return:
	string_res -- the files in a string format
	'''
	string_res = ''
	for file in os.listdir(path):
		string_res = string_res+path+'/'+file+','
	print('Files for the treatment: %s'%string_res[0:-1])
	return string_res[0:-1]

def splitInfos(line):
	'''
	Get only informations needed from the row with mid=

	Arguments:
	line -- the line where the data will be extracted

	Return:
	list -- the informations of the line mid=
	'''
	fields = line.split(';')
	tmp_dic = {}
	for row in fields:
		key,value = row.split('=')
		tmp_dic[key] = value

	mid = tmp_dic['mid']
	tref = tmp_dic['tref']
	tid = tmp_dic['tid']
	oid = tmp_dic['oid']
	ipAddr = tmp_dic['ipAddr']
	amount = tmp_dic['amount']
	captureMode = tmp_dic['captureMode']
	return mid,tref,tid,oid,ipAddr,amount,captureMode

def splitConv(line):
	'''
	Get only informations needed from the row conv

	Arguments:
	line -- the line where the data will be extracted

	Return:
	list -- the informations of the line conv
	'''
	fields = line.split(';')
	if len(fields) > 31:
		b_alias = fields[2]
		b_contract = fields[3].split('/')[1]
		m_alias = fields[4].split('/')[0]
		r_code = fields[13]
		r_label = fields[10]
		r_detailed = fields[31]
		p_mean = fields[22]
	else:
		b_alias = None
		b_contract = None
		m_alias = None
		r_code = None
		r_label = None
		r_detailed = None
		p_mean = None
	return b_alias,b_contract,m_alias,r_code,r_label,r_detailed,p_mean

def parse(line):
	'''
	Parse the line to get the right output

	Arguments:
	line -- the line where the data will be extracted

	Return:
	list -- the full extracted data
	'''
	fields = line.split('-')
	mid_index = indexes(fields, 'mid=')[0] if len(indexes(fields, 'mid=')) != 0 else 0
	conv_index = indexes(fields, 'CONVENTION')[0] if len(indexes(fields, 'CONVENTION')) !=0 else 0

	processID = re.sub('\(|\)','',str(fields[3])) if len(fields[3]) > 0 else None
	if (len(fields[mid_index]) > 0 and ('mid=' in fields[mid_index])):
		mid,tref,tid,oid,ipAddr,amount,captureMode = splitInfos(fields[mid_index])
	else :
		mid,tref,tid,oid,ipAddr,amount,captureMode = None,None,None,None,None,None,None

	if (len(fields) >= (conv_index+1) and ('CONVENTION' in fields[conv_index])):
		b_alias,b_contract,m_alias,r_code,r_label,r_detailed,p_mean = splitInfos(fields[conv_index])
	else
		b_alias,b_contract,m_alias,r_code,r_label,r_detailed,p_mean = None,None,None,None,None,None,None
	
	return processID,mid,tref,tid,oid,ipAddr,amount,captureMode,b_alias,b_contract,m_alias,r_code,r_label,r_detailed,p_mean

'''
Create and manage RDD
'''

rdd = spark.sparkContext.textFile(getFileList(path))
rdd_all = rdd.map(parse)


'''
Create and manage DF
'''
columns = StructType([\
	StructField('processID', StringType(),True),\
	StructField('mid', StringType(),True),\
	StructField('tref', StringType(),True),\
	StructField('tid', StringType(),True),\
	StructField('oid', StringType(),True),\
	StructField('ipAddr', StringType(),True),\
	StructField('amount', StringType(),True),\
	StructField('captureMode', StringType(),True),\
	StructField('b_alias', StringType(),True),\
	StructField('b_contract', StringType(),True),\
	StructField('m_alias', StringType(),True),\
	StructField('r_code', StringType(),True),\
	StructField('r_label', StringType(),True),\
	StructField('r_detailed', StringType(),True),\
	StructField('p_mean', StringType(),True)\
	])

df_mid_conv_init = spark.createDataFrame(data=rdd_all, schema=columns)
df_mid_conv = df_mid_conv_init.alias('mid').join(df_mid_conv_init.alias('conv'),\
										col('mid.processID') == col('conv.processID'), 'inner'
										)\
										.select(
											col('mid.processID'),\
											col('mid.mid'),\
											col('mid.tref'),\
											col('mid.tid'),\
											col('mid.oid'),\
											col('mid.ipAddr'),\
											col('mid.amount'),\
											col('mid.captureMode'),\
											col('conv.b_alias'),\
											col('conv.b_contract'),\
											col('conv.m_alias'),\
											col('conv.r_code'),\
											col('conv.r_label'),\
											col('conv.r_detailed'),\
											col('conv.p_mean')\
										)\
										.filter('r_code is not NULL and tref is not NULL')

print("Number of rows: %s"%(df_mid_conv.count()))
df_mid_conv.show(5)

print("Table's columns:")
df_mid_conv.printSchema()


'''
Process Data
'''
df_mid_conv.createOrReplaceView("Logs")

request = """
		select * 
		from Logs
	  """

spark.sql(request)\
	.show(truncate=False)

'''
Storage 
'''
spark.sql(request).write.mode('overwrite').options(header='True', delimiter=';')\
	.csv('results')
