from mrjob.job import MRJob


class Rating_count(MRJob):
        def mapper(self, _, line):
            (id,title,genres)=line.split(',')
            title=title.split(" ")
            year=title[-1]
            year=year[1:len(year)-1]
            yield(year,1)
        def reducer(self, year, c):
            yield(year, sum(c))

if __name__ == '__main__':
    Rating_count.run()