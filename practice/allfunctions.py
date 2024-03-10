
def read_csv(spark,path,delimiter=','):
	df=spark.read.format("CSV").option("header","true")\
    .option("inferschema","true").load(path)
	return df

def read_json(spark,path):
	df=spark.read.format('JSON').load(path)
	return df

def read_jdbc(spark,url,username,password,driver,table):
	df=spark.read.format('JDBC').option("url",url).option("user",username).option("password",password).option("driver",driver).option("table",table).load()
	return df

def write_csv(spark,df,path,mode='overwrite'):
	df.write.format("CSV").option("header","true").mode(mode).save(path)