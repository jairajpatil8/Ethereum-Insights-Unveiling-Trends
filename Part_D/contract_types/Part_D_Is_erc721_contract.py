from mrjob.job import MRJob

class erc721_contract(MRJob):
	def mapper(self, _, line):
		try:
			fields = line.split(',')
			if len(fields) == 5:
				if fields[2] == "true":
					yield fields [0], 1

		except:
			pass

	def reducer(self, u, t):
		yield (u,sum(t))

if __name__ == '__main__':
	erc721_contract.run()


# python Part_D_Is_erc721_contract.py -r hadoop --output-dir Part_D_Is_erc721_contract --no-cat-output hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/contracts/