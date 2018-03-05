from mrjob.job import MRJob
from numpy.random import uniform


'''
first, count total in sample
then, select w that probability 100/total

'''
class RandSample100(MRJob):
    def mapper_init(self):
        self.desired_samples = 100
        self.n_colleges = 1303  # result from count_total_colleges.py

    def mapper(self, _, line):
        sample = uniform()
        if sample <= self.desired_samples/self.n_colleges:
            yield ('_', line)

    def reducer(self, key, values):
        yield ('Uniformly sampled colleges: ', values)

if __name__ == '__main__':
    RandSample100.run()
