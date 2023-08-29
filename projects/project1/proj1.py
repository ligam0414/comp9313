import math
import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env

class ImportantTerms(MRJob):
                
    def mapper(self, _, line):
        date, texts = line.split(",")
        if len(texts) == 0:
            return
        words = texts.split(" ")
        year = date[0:4]
        output = {}

        for word in words:
            output[word] = output.get(word, 0) + 1
        
        for word, count in output.items():
            yield f'{word}#9999', year
            yield f'{word}#{year}', count

    def combiner(self, key, values):
        word, year = key.split("#")        
        if year == "9999":
            years = set()
            for y in values:
                years.add(y)
            for y in years:
                yield f'{word}#9999', y
        else:
            localtf = sum(values)
            yield f'{word}#{year}', localtf
            
    def reducer_init(self):
        self.YF=-1	#record current word in how many years
        self.cur_word = ""
        self.N = int(jobconf_from_env('myjob.settings.years'))
        self.beta = float(jobconf_from_env('myjob.settings.beta'))
        
            
    def reducer(self, key, values):        
        word, year = key.split("#")

        if year == "9999":
            years = set()
            for y in values:
                years.add(y)
            self.YF = len(years)            

            self.cur_word = word
        else:
            tf = sum(values)            
            tfidf = tf * math.log10(self.N/self.YF) 
            
            if tfidf >= self.beta:
                yield self.cur_word, year+","+ str(tfidf) 
   
    SORT_VALUES = True

    JOBCONF = {
      'map.output.key.field.separator': '#',
      'mapreduce.partition.keypartitioner.options':'-k1,1',
      'mapreduce.job.output.key.comparator.class':'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
      'mapreduce.partition.keycomparator.options':'-k1,1 -k2,2nr'
    }
           
if __name__ == '__main__':
    ImportantTerms.run()
