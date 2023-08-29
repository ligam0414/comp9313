from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import heapq
import sys

def takeTopK(pairList, k):
    topk = []
    heapq.heapify(topk) 
    for e in pairList:
        p = Pair(e[0], e[1])
        heapq.heappush(topk,p)
        if len(topk)>k:
            heapq.heappop(topk)
    sortedtopk = sorted(topk, reverse=True)
    resList = []
    for e in sortedtopk:
        resList.append(e.term+":"+str(e.count)) 
    return resList

def pairGen(x):
    result = []
    for i in range(len(x)):
        for j in range(i + 1, len(x)):
            if x[i] < x[j]:
                l = x[i]
                r = x[j]
            else:
                l = x[j]
                r = x[i]
            result.append(f'{l},{r}')
    return result

class Pair:
    def __init__(self, term, count):
        self.term = term
        self.count = count
    def __lt__(self, other):
        if self.count != other.count:
            return self.count < other.count
        else:
            return self.term > other.term


class Project2:   

    swlist = []
    
    def filterWords(self, x):
        res = []
        for i in x:
            if i not in self.swlist.value and i[0].isalpha():
                res.append(i)
        return res
        
    def run(self, inputPath, outputPath, stopwords, k):
        spark = SparkSession.builder.master("local").appName("project2_df").getOrCreate()
        
        fileDF = spark.read.text(inputPath)
        self.swlist = spark.sparkContext.broadcast(set(spark.sparkContext.textFile(stopwords).collect()))
        
        headlineDF = fileDF.select(split(fileDF['value'], ',').getItem(0).substr(0,4).alias('year'), split(fileDF['value'], ',').getItem(1).alias('headline'))
        headlineDF1 = headlineDF.filter(headlineDF.headline!='').withColumn('word',split('headline',' '))
        
        filterWordsUDF = udf(lambda x: self.filterWords(x), ArrayType(StringType()))
        headlineDF1 = headlineDF1.withColumn('word',filterWordsUDF(headlineDF1.word))        
        
        pairGenUDF = udf(lambda x: pairGen(x), ArrayType(StringType()))
        yearpairlistDF = headlineDF1.withColumn("pairs", pairGenUDF(headlineDF1.word)).select('year', 'pairs')
        
        yearpairDF = yearpairlistDF.select('year', explode(yearpairlistDF.pairs).alias('pair'))        
        yearpairDF = yearpairDF.groupBy('year', 'pair').count()
        
        groupDF = yearpairDF.groupBy('year').agg(collect_list(struct('pair', 'count')).alias('value'))               
        
        topkUDF = udf(lambda z,k: takeTopK(z,k), ArrayType(StringType()))
        result = groupDF.withColumn('value', topkUDF('value', lit(int(k)))).orderBy('year').select('year', explode('value').alias('paircount')) 
        result = result.coalesce(1).withColumn('result', concat_ws('\t', 'year', 'paircount')).select('result')
        result.write.text(outputPath)
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Wrong arguments")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])

