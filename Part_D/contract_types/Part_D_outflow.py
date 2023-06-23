from mrjob.job import MRJob

class contract(MRJob):
	def mapper(self, _, line):
		try:
			fields = line.split(",")
			if len(fields) == 7:
				yield fields[2], 1

		except:
			pass

	def reducer(self, k, v):
		yield k, sum(v)

if __name__ == '__main__':
	contract.run()


python Part_D_outflow.py -r hadoop --output-dir Part_D_outflow --no-cat-output hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions/