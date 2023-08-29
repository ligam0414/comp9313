from pyspark import SparkContext, SparkConf
import sys

Id = 0
PostId = 1
VoteTypeID = 2
UserId = 3
CreationDate = 4
    
class Problem3:

    def Question1(self, fields):
        #add your code here

    
    def Question2(self, fields):
    	#add your code here

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Wrong inputs")
        sys.exit(-1)

    conf = SparkConf().setAppName("problem3").setMaster("local")
    sc = SparkContext(conf=conf)   
    textFile = sc.textFile(sys.argv[1])
    fields = textFile.map(lambda line: line.split(","))
    p3 = Problem3()
    p3.Question1(fields)
    p3.Question2(fields)
    sc.stop()