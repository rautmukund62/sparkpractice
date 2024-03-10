def read_csv(spark,path,delimiter=','):
	"""
	this is function which can read csv file and return df
	call : read_csv(spark,path_file,delimiter)
	"""
	df=spark.read.format("CSV").option("header","true")\
    .option("inferschema","true").option("delimiter",delimiter).load(path)
	return df

def read_json(spark,path,multiline='false'):
	"""
	this is function which can read json file and return df
	call : read_json(spark,path_file,delimiter)
	"""
	df=spark.read.format("JSON").option("multiline",multiline).load(path)
	return df

def read_table(spark,url,username,password,table_name,driver):
	"""
	this is function which can read RDBMS and return df
	call : read_table(spark,url,username,password,table_name,driver)
	"""
	df=spark.read.format("JDBC").option("url",url).option("user",username).option("password",password).option("dbtable",table_name).option("driver",driver).load()
	return df
