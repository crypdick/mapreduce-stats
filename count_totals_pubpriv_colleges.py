from mrjob.job import MRJob



class CountTotalColleges(MRJob):
    '''
    python count_total_colleges.py Data/colleges.csv

    Output: "total colleges:"       1303

    '''
    def mapper(self, _, line):
        yield ('_', 1)

    def reducer(self, _, values):
        count = 0
        for v in values:
            count += 1
        yield ('total colleges:', count)

if __name__ == '__main__':
    result = CountTotalColleges.run()
    print(result)