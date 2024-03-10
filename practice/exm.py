from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from allfunctions import *
from sys import argv

import json
from datetime import date

todays_date = date.today()
print(todays_date)



spark=SparkSession.builder.appName("Sample").getOrCreate()

config_file_path = 'D://config.json'

def read_config_from_json(json_file):
    with open(json_file, 'r') as file:
        config = json.load(file)
    return config

config_data = read_config_from_json(config_file_path)

# Access parameters from the config data
input_path = config_data.get("input_path", "")
output_path = config_data.get("output_path", "")

print(input_path)
print(output_path)

#read
df=read_csv(spark,input_path)


#process
df1=df.filter(upper(df['Genre'])=='ACTION')

#tgt
write_csv(spark,df1,output_path)

#validation
#count

dfs=read_csv(spark,input_path)
dft=read_csv(spark,output_path)

src_cnt=dfs.filter(upper(dfs['Genre'])=='ACTION').count()
tgt_cnt=dft.count()

if src_cnt==tgt_cnt:
    print("Count Validation Pass")
    cstatus="Pass"
else:
    print("Count Validation Failed")
    cstatus = "Failed"


def handle_datatype(list):
    list=str(list).replace('decimal(5,0)','int')
    return list

src=dfs.dtypes
tgt=dft.dtypes

src=handle_datatype(src)


print(src)
print(tgt)

print(type(src))
print(type(tgt))

if src==str(tgt):
    print("Schema Validation Pass")
    sstatus="Pass"
else:
    print("Schema Validation Failed")
    sstatus="Failed"

with open("D:/validation.txt","a") as file:
    file.write("Validation report generated on: {}".format(todays_date))
    file.write("\n #####Count Validation##### \n")
    file.write("Src Count : {} Target Count: {} Validation {} \n".format(src_cnt,tgt_cnt,cstatus))
    file.write("SRC Schema : {} \n".format(src))
    file.write("TGT Schema : {} \n".format(tgt))
    file.write("Count Validation Completed \n")
    file.write("Schema Validation Started \n")
    file.write("Schema Validation STatus : {} \n".format(sstatus))
    file.write("Schema Validation Completed \n")

file.close()

print("Program completed")