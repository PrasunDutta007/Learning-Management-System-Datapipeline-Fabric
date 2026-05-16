# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## 01. Raw To Landing

# PARAMETERS CELL ********************

today_file = 'file'  #'LMS_09-01-2023.csv'
processed_date = '9999-99-99'  #'2026-05-14'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

account_name = 'fabricstorageprasun' # fill in your primary account name
container_name = 'lmsfabricproject' # fill in your container name
relative_path = 'raw' # fill in your relative folder path

adls_path = 'abfss://%s@%s.dfs.core.windows.net/%s' % (container_name, account_name, relative_path) 

print('Source storage account path is ', adls_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Reading CSV file of Today

# CELL ********************

latest_path = f"{adls_path}/{today_file}"
print(latest_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import lit
latest_path = f"{adls_path}/{today_file}"
df = spark.read.csv(path= latest_path, header=True, inferSchema= True)
display(df)

if df.count() > 1:
    print("The file has data.")
    
    df_new = df.withColumn("Processing_Date",lit(processed_date))
    df_new.write.format('csv').option('header','true').partitionBy('Processing_Date').mode('append').save('abfss://lmsfabricproject@fabricstorageprasun.dfs.core.windows.net/landing/')
    print("Data written to landing zone successfully !")

else:
    print('This file contains only header row and no data.')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
