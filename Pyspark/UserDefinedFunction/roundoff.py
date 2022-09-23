# Round of specific columns values to specific values after decimal. 

from pyspark.sql.functions import round, col
import pyspark.sql.functions as func

def round_off(df, col_list):
    #round off two deminals will happen
    print("*****************", col_list)
    dataframe = df.select(col_list)
    for c in dataframe.columns:
        print("*****", c)
        df = df.withColumn(c, func.round(df[c], 2).cast('float'))  #restricting it to 2 decimal points

    df.show(5)
    df.printSchema()

    return df
  roundoff_cols=['col1','col2']
  df = spark.read.format("csv").load("/tmp/resources/codes.csv")
  roundoff_data = round_off(df, roundoff_cols)
  
  ## 23.650004 will change to 23.65
