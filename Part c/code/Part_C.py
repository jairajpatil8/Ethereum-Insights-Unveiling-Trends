from mrjob.job import MRJob
import time

class partc(MRJob):

    def mapper(self,_,line):
        try:
            fields = line.split(',')
            if len(fields) == 9:
                size = float(fields[4])
                miner = fields[2]
                yield (fields[2], size)
        except:
            pass

    def combiner(self,key,val):
        yield (key,sum(val))

    def reducer(self,key,val):
        yield (key,sum(val))


if __name__=='__main__':
    partc.run()


# python partc.py -r hadoop --output-dir partc --no-cat-output hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions/part-*.csv

# job: http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1648683650522_7045/