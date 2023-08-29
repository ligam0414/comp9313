from pyspark import SparkContext, SparkConf
from heapq import nsmallest
import sys

def pairGenerate(L, year):
    result = []
    for i in range(len(L)):
        for j in range(i + 1, len(L)):
            if L[i] < L[j]:
                l = L[i]
                r = L[j]
            else:
                l = L[j]
                r = L[i]
            result.append((year, l+","+r))
    return result

class Project2:   
        
    def run(self, inputPath, outputPath, stopwords, k):
        conf = SparkConf().setAppName("project2_rdd")
        sc = SparkContext(conf=conf)
        
        filerdd = sc.textFile(inputPath)
        swlist = sc.broadcast(set(sc.textFile(stopwords).collect()))
        headlines = filerdd.map(lambda x: x.split(",", 1)).filter(lambda x: len(x[1])>0).map(lambda x: (x[0][:4], x[1].split(" ")))
        headlines1 = headlines.mapValues(lambda x: [item for item in x if item[0].isalpha()])
        headlines2 = headlines1.mapValues(lambda x: [item for item in x if item not in swlist.value])        
        
        paircount = headlines2.flatMap(lambda x: pairGenerate(x[1], x[0])).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
        yearpairlist = paircount.map(lambda x: (x[0][0], (x[0][1], x[1]))).groupByKey()
        yeartopklist = yearpairlist.mapValues(lambda x:nsmallest(int(k), x, key = lambda x:(-x[1], x[0]))).sortByKey()
        
        result = yeartopklist.flatMapValues(lambda x: x).map(lambda x: f'{x[0]}\t{x[1][0]}:{x[1][1]}')
        result.coalesce(1).saveAsTextFile(outputPath)
        sc.stop()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Wrong arguments")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])

