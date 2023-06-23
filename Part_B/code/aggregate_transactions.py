from mrjob.job import MRJob
from mrjob.step import MRStep
import fileinput
import sys
from time import gmtime , strftime , struct_time
from datetime import datetime
 
class trans_per_m(MRJob):

 def mapper(self, _,line):
  try:
   fields = line.split(',')
   if len(fields)==7:
    to_address=fields[2]
    value=int(fields[3])
    yield(to_address,value)

  except:
   pass

 def reducer(self,to_address,value):
  yield(to_address, sum(value))


if __name__ == "__main__":
 
 trans_per_m.run()

# python agg_trans.py -r hadoop --output-dir agg_trans --no-cat-output hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions/part-*.csv

# job: http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1649894236110_0769/