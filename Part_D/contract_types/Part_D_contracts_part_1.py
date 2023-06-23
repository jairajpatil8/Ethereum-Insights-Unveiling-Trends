from mrjob.job import MRJob

class number_of_transactions(MRJob):
	def mapper(self, _, line):
		try:
			fields = line.split(",")
			if len(fields) == 7:
				key = fields[2]
				yield (key, 1)

		except:
			pass

	def combiner(self, k, l):
		yield k, sum(l)

	def reducer(self, u, t):
		yield (u, sum(t))

if __name__ == '__main__':	
	number_of_transactions.run()
