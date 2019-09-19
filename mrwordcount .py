from mrjob.job import MRJob
from mrjob.step import MRStep

class WordCount(MRJob):
    def mapper(self, key, line):
        word =[k.lower() for k in line.split()]
        for w in word:
            yield (w, 1)

    def mapper_(self,key,value):
        yield(value,1)

    def reducer(self, key, values):
        yield (key, sum(values))

    def reducer_(self,key,values):
       yield(key,sum(values))

    def steps(self):
        return[
           MRStep(mapper=self.mapper,reducer=self.reducer),
           MRStep(mapper=self.mapper_,reducer=self.reducer_)
        ]

if __name__ == '__main__':
    WordCount.run()
