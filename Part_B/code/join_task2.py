from mrjob.job import MRJob
from mrjob.step import MRStep
import fileinput
import sys
from time import gmtime , strftime , struct_time
from datetime import datetime

class transaction_per_month(MRJob):

    def mapper(self,_,line):
     try:
      if (len(line.split('\t'))==2):
       fields = line.split('\t')
       join_key = fields[0]
       join_val = int(fields[1])
       yield (join_key,(join_val,1))

      elif(len(line.split(','))==5):
       fields = line.split(',')
       join_key = fields[0]
       join_val = fields[3]
       yield (join_key, (join_val, 2))

     except:
      pass

    def reducer(self, address, values):
     contracts =[]
     aggregate_file = None

     for value in values:
      if value[1]==1:
       aggregate_file=value[0]
      elif value[1]==2:
       contracts.append(value[0])
      if aggregate_file != None and len(contracts)!=0:
       yield (address, aggregate_file)


if __name__ == "__main__":

 transaction_per_month.run()


 # python join_task2.py -r hadoop --output-dir joined_records --no-cat-output hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/contracts/ hdfs://andromeda.eecs.qmul.ac.uk/user/jmp01/agg_trans/

