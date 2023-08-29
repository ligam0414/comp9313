from pyspark.sql.session import SparkSession
import sys

class Problem3:

    def Question1(self, fields):
        # add your code here

    
    def Question2(self, fields):
    	# add your code here

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Wrong inputs")
        sys.exit(-1)

    inputFile = sys.argv[1]
    spark = SparkSession.builder.master("local").appName("problem3").getOrCreate()
    fields = spark.read.csv(inputFile).toDF("Id", "PostId", "VoteTypeId", "UserId", "CreationTime")

    p3 = Problem3()
    p3.Question1(fields)
    p3.Question2(fields)
    spark.stop()






