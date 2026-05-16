# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0e8bd157-9c70-4ae3-a3e3-a86ff09b8cad",
# META       "default_lakehouse_name": "LH_Gold",
# META       "default_lakehouse_workspace_id": "6ac36fa9-36c6-4030-8fce-ad3a3e20b795",
# META       "known_lakehouses": [
# META         {
# META           "id": "0e8bd157-9c70-4ae3-a3e3-a86ff09b8cad"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## 04. Gold Layer

# PARAMETERS CELL ********************

today_date = '9999-99-99'  #'2026-05-14'
workspace =  'NA'   #"fabric_DEV"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Reading Silver Table

# CELL ********************

from pyspark.sql.functions import col

df = spark.read.format('delta').load(f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/LH_Silver.Lakehouse/Tables/silver_data").filter(col("Processing_Date") == str(today_date))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Creating Dimension Tables

# MARKDOWN ********************

# ##### Creating Dim_student Table

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from delta.tables import DeltaTable

# Define schemas for dimension tables
dim_student_schema = StructType([
    StructField("Student_ID", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Gender", StringType(), True),
    StructField("Demographic_Group", StringType(), True),
    StructField("Internet_Access", StringType(), True),
    StructField("Learning_Disabilities", StringType(), True),
    StructField("Preferred_Learning_Style", StringType(), True),
    StructField("Language_Proficiency", StringType(), True),
    StructField("Parent_Involvement", StringType(), True)
])

DeltaTable.createIfNotExists(spark).tableName("Dim_student")\
          .addColumns(dim_student_schema)\
          .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Creating Dim_course Table

# CELL ********************

# Dim_Course Schema
dim_course_schema = StructType([
    StructField("Course_ID", StringType(), True),
    StructField("Course_Name", StringType(), True),
    StructField("Grade_Level", StringType(), True)
])

DeltaTable.createIfNotExists(spark).tableName('Dim_Course')\
          .addColumns(dim_course_schema)\
          .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Creating Fact_Student_performance

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# Define schema for fact table
fact_student_performance_schema = StructType([
    StructField("Student_ID", StringType(), True),
    StructField("Course_ID", StringType(), True),
    StructField("Enrollment_Date", DateType(), True),
    StructField("Completion_Date", DateType(), True),
    StructField("Status", StringType(), False),
    StructField("Final_Grade", StringType(), False),
    StructField("Attendance_Rate", DoubleType(), False),
    StructField("Time_Spent_on_Course_hrs", DoubleType(), False),
    StructField("Assignments_Completed", IntegerType(), False),
    StructField("Quizzes_Completed", IntegerType(), False),
    StructField("Forum_Posts", IntegerType(), False),
    StructField("Messages_Sent", IntegerType(), False),
    StructField("Quiz_Average_Score", DoubleType(), False),
    StructField("Assignment_Scores", StringType(), True),
    StructField("Assignment_Average_Score", DoubleType(), False),
    StructField("Project_Score", DoubleType(), False),
    StructField("Extra_Credit", DoubleType(), False),
    StructField("Overall_Performance", DoubleType(), False),
    StructField("Feedback_Score", DoubleType(), False),
    StructField("Completion_Time_Days", IntegerType(), True),
    StructField("Performance_Score", DoubleType(), False),
    StructField("Course_Completion_Rate", StringType(), False),
    StructField("Processing_Date", DateType(), True)
])

DeltaTable.createIfNotExists(spark).tableName("fact_student_performance")\
           .addColumns(fact_student_performance_schema)\
           .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Loading Data into Dim_student

# CELL ********************

df_selected_dim_student = (df.select( "Student_ID", 
    "Name", 
    "Age", 
    "Gender", 
    "Demographic_Group", 
    "Internet_Access", 
    "Learning_Disabilities", 
    "Preferred_Learning_Style", 
    "Language_Proficiency", 
    "Parent_Involvement"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import *

dim_student_table_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/LH_gold.Lakehouse/Tables/dim_student"
dim_deltastudent = DeltaTable.forPath(spark,dim_student_table_path)

## Perform the MERGE (UPSERT)

dim_deltastudent.alias('target').merge(
    df_selected_dim_student.alias('source'),
    "target.Student_ID = source.Student_ID"
).whenMatchedUpdate( set = {
    "Name": "source.Name",
    "Age": "source.Age",
    "Gender": "source.Gender",
    "Demographic_Group": "source.Demographic_Group",
    "Internet_Access": "source.Internet_Access",
    "Learning_Disabilities": "source.Learning_Disabilities",
    "Preferred_Learning_Style": "source.Preferred_Learning_Style",
    "Language_Proficiency": "source.Language_Proficiency",
    "Parent_Involvement": "source.Parent_Involvement"
}).whenNotMatchedInsert(values = {
    "Student_ID": "source.Student_ID",
    "Name": "source.Name",
    "Age": "source.Age",
    "Gender": "source.Gender",
    "Demographic_Group": "source.Demographic_Group",
    "Internet_Access": "source.Internet_Access",
    "Learning_Disabilities": "source.Learning_Disabilities",
    "Preferred_Learning_Style": "source.Preferred_Learning_Style",
    "Language_Proficiency": "source.Language_Proficiency",
    "Parent_Involvement": "source.Parent_Involvement"

}).execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get the history of the Delta table to extract metrics
history_df = dim_deltastudent.history(1)  # Get the latest operation

# Extract metrics from the history DataFrame
operation_metrics = history_df.select("operationMetrics").collect()[0][0]

# Extract specific metrics
rows_inserted = operation_metrics.get('numTargetRowsInserted', 0)
rows_updated = operation_metrics.get('numTargetRowsUpdated', 0)
rows_deleted = operation_metrics.get('numTargetRowsDeleted', 0)
rows_affected = int(rows_inserted) + int(rows_updated) + int(rows_deleted) 

print('Total rows of table: ',dim_deltastudent.toDF().count())
print("Merge Metrics:")
print(f"Rows inserted: {rows_inserted}")
print(f"Rows updated: {rows_updated}")
print(f"Rows deleted: {rows_deleted}")
print(f"Total rows affected: {rows_affected}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT * FROM LH_gold.dim_student LIMIT 1000

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Loading Data into Dim_course

# CELL ********************

# Selecting required column for Dim_course
df_selected_dim_course = (df.select( "Course_ID",
                                     "Course_Name",
                                     "Grade_Level"
                                    )
                         )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import *

dim_course_table_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/LH_gold.Lakehouse/Tables/dim_course"
dim_deltacourse = DeltaTable.forPath(spark,dim_course_table_path)

# Perform the MERGE (UPSERT) operation using PySpark Syntax

dim_deltacourse.alias("target").merge(
    df_selected_dim_course.alias("source"),
    "target.Course_ID = source.Course_ID"
).whenMatchedUpdate(set={
    "Course_ID": "source.Course_ID",
    "Course_Name": "source.Course_Name",
    "Grade_Level": "source.Grade_Level"
}).whenNotMatchedInsert(values={
     "Course_ID": "source.Course_ID",
    "Course_Name": "source.Course_Name",
    "Grade_Level": "source.Grade_Level"
}).execute()


##  To get history


# Get the history of the Delta table to extract metrics
history_df = dim_deltacourse.history(1)  # Get the latest operation

# Extract metrics from the history DataFrame
operation_metrics = history_df.select("operationMetrics").collect()[0][0]

# Extract specific metrics
rows_inserted = operation_metrics.get('numTargetRowsInserted', 0)
rows_updated = operation_metrics.get('numTargetRowsUpdated', 0)
rows_deleted = operation_metrics.get('numTargetRowsDeleted', 0)
rows_affected = int(rows_inserted) + int(rows_updated) + int(rows_deleted)

print('Total rows of table: ',dim_deltacourse.toDF().count())
print("Merge Metrics:")
print(f"Rows inserted: {rows_inserted}")
print(f"Rows updated: {rows_updated}")
print(f"Rows deleted: {rows_deleted}")
print(f"Total rows affected: {rows_affected}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Loading data into Fact_student_performance

# CELL ********************

df_selected_Fact_student = (df.select( 
    "Student_ID",
    "Course_ID",
    "Enrollment_Date",
    "Completion_Date",
    "Status",
    "Final_Grade",
    "Attendance_Rate",
    "Time_Spent_on_Course_hrs",
    "Assignments_Completed",
    "Quizzes_Completed",
    "Forum_Posts",
    "Messages_Sent",
    "Quiz_Average_Score",
    "Assignment_Scores",
    "Assignment_Average_Score",
    "Project_Score",
    "Extra_Credit",
    "Overall_Performance",
    "Feedback_Score",
    "Completion_Time_Days",
    "Performance_Score",
    "Course_Completion_Rate",
    "Processing_Date"
))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import *

fact_student_table_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/LH_gold.Lakehouse/Tables/fact_student_performance"
fact_deltastudent = DeltaTable.forPath(spark,fact_student_table_path)

# Perform the MERGE (UPSERT) operation and capture the operation metrics
merge_operation = fact_deltastudent.alias("target").merge(
    df_selected_Fact_student.alias("source"),
    "target.Student_ID = source.Student_ID AND target.Course_ID = source.Course_ID"
).whenMatchedUpdate(set={
    "Student_ID": "source.Student_ID",
    "Course_ID": "source.Course_ID",
    "Enrollment_Date": "source.Enrollment_Date",
    "Completion_Date": "source.Completion_Date",
    "Status": "source.Status",
    "Final_Grade": "source.Final_Grade",
    "Attendance_Rate": "source.Attendance_Rate",
    "Time_Spent_on_Course_hrs": "source.Time_Spent_on_Course_hrs",
    "Assignments_Completed": "source.Assignments_Completed",
    "Quizzes_Completed": "source.Quizzes_Completed",
    "Forum_Posts": "source.Forum_Posts",
    "Messages_Sent": "source.Messages_Sent",
    "Quiz_Average_Score": "source.Quiz_Average_Score",
    "Assignment_Scores": "source.Assignment_Scores",
    "Assignment_Average_Score": "source.Assignment_Average_Score",
    "Project_Score": "source.Project_Score",
    "Extra_Credit": "source.Extra_Credit",
    "Overall_Performance": "source.Overall_Performance",
    "Feedback_Score": "source.Feedback_Score",
    "Completion_Time_Days": "source.Completion_Time_Days",
    "Performance_Score": "source.Performance_Score",
    "Course_Completion_Rate": "source.Course_Completion_Rate",
    "Processing_Date": "source.Processing_Date"
}).whenNotMatchedInsert(values={
    "Student_ID": "source.Student_ID",
    "Course_ID": "source.Course_ID",
    "Enrollment_Date": "source.Enrollment_Date",
    "Completion_Date": "source.Completion_Date",
    "Status": "source.Status",
    "Final_Grade": "source.Final_Grade",
    "Attendance_Rate": "source.Attendance_Rate",
    "Time_Spent_on_Course_hrs": "source.Time_Spent_on_Course_hrs",
    "Assignments_Completed": "source.Assignments_Completed",
    "Quizzes_Completed": "source.Quizzes_Completed",
    "Forum_Posts": "source.Forum_Posts",
    "Messages_Sent": "source.Messages_Sent",
    "Quiz_Average_Score": "source.Quiz_Average_Score",
    "Assignment_Scores": "source.Assignment_Scores",
    "Assignment_Average_Score": "source.Assignment_Average_Score",
    "Project_Score": "source.Project_Score",
    "Extra_Credit": "source.Extra_Credit",
    "Overall_Performance": "source.Overall_Performance",
    "Feedback_Score": "source.Feedback_Score",
    "Completion_Time_Days": "source.Completion_Time_Days",
    "Performance_Score": "source.Performance_Score",
    "Course_Completion_Rate": "source.Course_Completion_Rate",
    "Processing_Date": "source.Processing_Date"
}).execute()


## History 

# Get the history of the Delta table to extract metrics
history_df = fact_deltastudent.history(1)  # Get the latest operation

# Extract metrics from the history DataFrame
operation_metrics = history_df.select("operationMetrics").collect()[0][0]

# Extract specific metrics
rows_inserted = operation_metrics.get('numTargetRowsInserted', 0)
rows_updated = operation_metrics.get('numTargetRowsUpdated', 0)
rows_deleted = operation_metrics.get('numTargetRowsDeleted', 0)
rows_copied = operation_metrics.get('numTargetRowsCopied', 0)
rows_affected = int(rows_inserted) + int(rows_updated) + int(rows_deleted) + int(rows_copied)

print('Total rows of table: ',fact_deltastudent.toDF().count())
print("Merge Metrics:")
print(f"Rows inserted: {rows_inserted}")
print(f"Rows updated: {rows_updated}")
print(f"Rows deleted: {rows_deleted}")
#print(f"Rows copied: {rows_copied}")
print(f"Total rows affected: {rows_affected}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT * FROM LH_gold.fact_student_performance LIMIT 1000

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 
