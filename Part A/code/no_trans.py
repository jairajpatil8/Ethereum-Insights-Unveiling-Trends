from mrjob.job import MRJob
from datetime import datetime
import time
class no_trans(MRJob):

	def mapper(self, _, line):
		try:
			fields = line.split(',')
			if len(fields) == 7:
				day = (datetime.utcfromtimestamp(int(fields[6])).strftime('%Y-%m'))
				value = int(fields[3])
				yield (day,1)

		except :
			pass
	def reducer(self,u,t):
		yield u, sum(t)

if __name__ == '__main__':
	no_trans.run()


# python no_trans.py -r hadoop --output-dir trans_per_month --no-cat-output hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions/part-*.csv

# job: http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1648683650522_6967/