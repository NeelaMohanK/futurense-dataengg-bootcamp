from mrjob.job import MRJob


class Rating_count(MRJob):

        """
        The mapper function splits  the line and generates a word with 
        count 1 """
        def mapper(self, _, line):
        	#print(line.split('\t'))
                (userID,movieID, rating, timestamp) = line.split(',')
                yield(rating, 1)
                        
        """ The  reducer() is aggregating the result according to their key and
        producing the output in a key-value format with its total count"""
        def reducer(self, rate, counts):
                yield(rate, sum(counts))
if __name__ == '__main__':
	Rating_count.run()