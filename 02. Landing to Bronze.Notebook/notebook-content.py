# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e44da076-2903-48cf-afb1-c641fa9df2ca",
# META       "default_lakehouse_name": "LH_Bronze",
# META       "default_lakehouse_workspace_id": "6ac36fa9-36c6-4030-8fce-ad3a3e20b795",
# META       "known_lakehouses": [
# META         {
# META           "id": "e44da076-2903-48cf-afb1-c641fa9df2ca"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## 02. Landing to Bronze

# PARAMETERS CELL ********************

today_date = '9999-09-09' #'2026-05-16'
workspace =  'NA'  #'fabric_DEV'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Defining ADLS Landing Zone Path

# CELL ********************

account_name = 'fabricstorageprasun' # fill in your primary account name
container_name = 'lmsfabricproject' # fill in your container name
relative_path = 'landing' # fill in your relative folder path

adls_path = 'abfss://%s@%s.dfs.core.windows.net/%s' % (container_name, account_name, relative_path) 
partition_path = f"/Processing_Date={today_date}/"

complete_path = adls_path + partition_path
print('Source storage account path is ', complete_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

# Define the schema
schema = StructType([
    StructField("Student_ID", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Gender", StringType(), True),
    StructField("Grade_Level", StringType(), True),
    StructField("Course_ID", StringType(), True),
    StructField("Course_Name", StringType(), True),
    StructField("Enrollment_Date", StringType(), True),
    StructField("Completion_Date", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("Final_Grade", StringType(), True),
    StructField("Attendance_Rate", DoubleType(), True),
    StructField("Time_Spent_on_Course_hrs", DoubleType(), True),
    StructField("Assignments_Completed", IntegerType(), True),
    StructField("Quizzes_Completed", IntegerType(), True),
    StructField("Forum_Posts", IntegerType(), True),
    StructField("Messages_Sent", IntegerType(), True),
    StructField("Quiz_Average_Score", DoubleType(), True),
    StructField("Assignment_Scores", StringType(), True),
    StructField("Assignment_Average_Score", DoubleType(), True),
    StructField("Project_Score", DoubleType(), True),
    StructField("Extra_Credit", DoubleType(), True),
    StructField("Overall_Performance", DoubleType(), True),
    StructField("Feedback_Score", DoubleType(), True),
    StructField("Parent_Involvement", StringType(), True),
    StructField("Demographic_Group", StringType(), True),
    StructField("Internet_Access", StringType(), True),
    StructField("Learning_Disabilities", StringType(), True),
    StructField("Preferred_Learning_Style", StringType(), True),
    StructField("Language_Proficiency", StringType(), True),
    StructField("Participation_Rate", StringType(), True),
    StructField("Completion_Time_Days", IntegerType(), True),
    StructField("Performance_Score", DoubleType(), True),
    StructField("Course_Completion_Rate", DoubleType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Creating new_data view (from landing folder)

# CELL ********************

df = spark.read.format('csv').option('header','true').schema(schema).load(complete_path)

print("Reading data of : ", partition_path)

df.createOrReplaceTempView('new_data')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Creating Empty Bronze Table

# CELL ********************

fabric_bronze_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/LH_Bronze.Lakehouse/Tables/bronze_data"

try:
    spark.read.format('delta').load(fabric_bronze_path).createOrReplaceTempView('bronze_data')
except:
    create_table = f"""CREATE TABLE IF NOT EXISTS bronze_data (
        Student_ID STRING,
        Name STRING,
        Age INT,
        Gender STRING,
        Grade_Level STRING,
        Course_ID STRING,
        Course_Name STRING,
        Enrollment_Date STRING,
        Completion_Date STRING,
        Status STRING,
        Final_Grade STRING,
        Attendance_Rate DOUBLE,
        Time_Spent_on_Course_hrs DOUBLE,
        Assignments_Completed INT,
        Quizzes_Completed INT,
        Forum_Posts INT,
        Messages_Sent INT,
        Quiz_Average_Score DOUBLE,
        Assignment_Scores STRING,
        Assignment_Average_Score DOUBLE,
        Project_Score DOUBLE,
        Extra_Credit DOUBLE,
        Overall_Performance DOUBLE,
        Feedback_Score DOUBLE,
        Parent_Involvement STRING,
        Demographic_Group STRING,
        Internet_Access STRING,
        Learning_Disabilities STRING,
        Preferred_Learning_Style STRING,
        Language_Proficiency STRING,
        Participation_Rate STRING,
        Completion_Time_Days INT,
        Performance_Score DOUBLE,
        Course_Completion_Rate DOUBLE,
        Processing_Date DATE )"""

    spark.sql(create_table)
    spark.read.format('delta').load(fabric_bronze_path).createOrReplaceTempView('bronze_data')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT * FROM new_data

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT * FROM bronze_data

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Upsert Logic for Inserting / Updating Data

# CELL ********************

sql_statement = f""" MERGE INTO bronze_data AS target
                    USING new_data AS source
                    ON target.Student_ID = source.Student_ID AND target.Course_ID = source.Course_ID
                    
                    WHEN MATCHED THEN
                        UPDATE SET
                            target.Name = source.Name,
                            target.Age = source.Age,
                            target.Gender = source.Gender,
                            target.Grade_Level = source.Grade_Level,
                            target.Course_Name = source.Course_Name,
                            target.Enrollment_Date = source.Enrollment_Date,
                            target.Completion_Date = source.Completion_Date,
                            target.Status = source.Status,
                            target.Final_Grade = source.Final_Grade,
                            target.Attendance_Rate = source.Attendance_Rate,
                            target.Time_Spent_on_Course_hrs = source.Time_Spent_on_Course_hrs,
                            target.Assignments_Completed = source.Assignments_Completed,
                            target.Quizzes_Completed = source.Quizzes_Completed,
                            target.Forum_Posts = source.Forum_Posts,
                            target.Messages_Sent = source.Messages_Sent,
                            target.Quiz_Average_Score = source.Quiz_Average_Score,
                            target.Assignment_Scores = source.Assignment_Scores,
                            target.Assignment_Average_Score = source.Assignment_Average_Score,
                            target.Project_Score = source.Project_Score,
                            target.Extra_Credit = source.Extra_Credit,
                            target.Overall_Performance = source.Overall_Performance,
                            target.Feedback_Score = source.Feedback_Score,
                            target.Parent_Involvement = source.Parent_Involvement,
                            target.Demographic_Group = source.Demographic_Group,
                            target.Internet_Access = source.Internet_Access,
                            target.Learning_Disabilities = source.Learning_Disabilities,
                            target.Preferred_Learning_Style = source.Preferred_Learning_Style,
                            target.Language_Proficiency = source.Language_Proficiency,
                            target.Participation_Rate = source.Participation_Rate,
                            target.Completion_Time_Days = source.Completion_Time_Days,
                            target.Performance_Score = source.Performance_Score,
                            target.Course_Completion_Rate = source.Course_Completion_Rate,
                            target.Processing_Date = '{today_date}'

                    WHEN NOT MATCHED THEN
                    INSERT ( Student_ID, Name, Age, Gender, Grade_Level, Course_ID, Course_Name, Enrollment_Date, Completion_Date,
                                Status, Final_Grade, Attendance_Rate, Time_Spent_on_Course_hrs, Assignments_Completed, Quizzes_Completed,
                                Forum_Posts, Messages_Sent, Quiz_Average_Score, Assignment_Scores, Assignment_Average_Score, Project_Score,
                                Extra_Credit, Overall_Performance, Feedback_Score, Parent_Involvement, Demographic_Group, Internet_Access,
                                Learning_Disabilities, Preferred_Learning_Style, Language_Proficiency, Participation_Rate, Completion_Time_Days,
                                Performance_Score, Course_Completion_Rate, Processing_Date)
                                VALUES 
                                ( source.Student_ID, source.Name, source.Age, source.Gender, source.Grade_Level, source.Course_ID, source.Course_Name, 
                                source.Enrollment_Date, source.Completion_Date, source.Status, source.Final_Grade, source.Attendance_Rate, 
                                source.Time_Spent_on_Course_hrs, source.Assignments_Completed, source.Quizzes_Completed, source.Forum_Posts, 
                                source.Messages_Sent, source.Quiz_Average_Score, source.Assignment_Scores, source.Assignment_Average_Score, 
                                source.Project_Score, source.Extra_Credit, source.Overall_Performance, source.Feedback_Score, source.Parent_Involvement, 
                                source.Demographic_Group, source.Internet_Access, source.Learning_Disabilities, source.Preferred_Learning_Style, 
                                source.Language_Proficiency, source.Participation_Rate, source.Completion_Time_Days, source.Performance_Score, 
                                source.Course_Completion_Rate, '{today_date}') 
                    """

spark.sql(sql_statement).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT * FROM LH_Bronze.bronze_data LIMIT 1000

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 

