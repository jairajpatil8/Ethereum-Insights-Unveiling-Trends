from mrjob.job import MRJob

class top10_miner(MRJob):

 def mapper(self, _,line):
  try:
   fields = line.split('\t')
   if len(fields) == 2:
    address = fields[0]
    aggregate = float(fields[1])
    yield (None,(address,aggregate))
  except:
   pass

 def combiner(self, _, values):
  sorted_values = sorted(values, reverse = True, key = lambda x:x[1])
  for idx,value in enumerate(sorted_values):
   yield ("top", value)
   if idx >= 10:
    break


 def reducer(self, _, values):

  sorted_values = sorted(values, reverse = True, key = lambda x:x[1])
  for idx,value in enumerate(sorted_values):
   yield ("{} - {}".format(value[0],value[1]),None)
   if idx >= 10:
    break


if __name__ == '__main__':
 top10_miner.run()

# python part_c_top_10.py inputfor10.txt > part_c_top10.txt