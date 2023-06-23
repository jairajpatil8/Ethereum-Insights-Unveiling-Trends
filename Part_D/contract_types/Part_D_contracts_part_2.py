from mrjob.job import MRJob

class number_of_transactions(MRJob):
	def mapper(self)
		try:
			fields = line.split()
			key = fields[0]
			value = int(fields[1])
			yield (None, (key, value))

		except:
			pass

	def reducer(self, _, val):
		sorted_values = sorted(val, reverse = True, key = lambda tup: tup[1])
		j =0
		for i in sprted_values:
			yield i
			j += 1
			if j >= 5:
				break

if _ _name_ _ == '_ _main_ _'
	number_of_transacions.run()


 # python Part_D_contracts_part_2.py -r hadoop --output-dir Part_D_contracts_part_2 --no-cat-output hdfs://andromeda.eecs.qmul.ac.uk/user/jmp01/Part_D_contracts_part_1/