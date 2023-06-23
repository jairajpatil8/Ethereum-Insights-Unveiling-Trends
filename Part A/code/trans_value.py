from mrjob.job import MRJob
from mrjob.step import MRStep
import fileinput
import sys
from time import gmtime , strftime , struct_time
from datetime import datetime
 
class trans_value(MRJob):

 def mapper(self, _,line):
  try:
   fields = line.split(',')
   if len(fields)==7:
    timestamp_val=(datetime.utcfromtimestamp(int(fields[6])).strftime(" %b,%Y "))
    value=int(fields[3])
    yield(timestamp_val,value)

  except:
   pass

 def combiner(self,timestamp_val,value):
  yield(timestamp_val,sum(value))

 def reducer(self,timestamp_val,value):
  yield(timestamp_val, sum(value))


if __name__ == "__main__":
 
 trans_value.run()


# python trans_value.py -r hadoop --output-dir each_month_transaction --no-cat-output hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions/part-*.csv

# job: http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1648683650522_7013/