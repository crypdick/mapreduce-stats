from mrjob.job import MRJob
from numpy.random import uniform



class WeightedSample100(MRJob):
    def mapper_init(self):
        self.expected_n_per_pubpriv = 50
        # result from count_totals_pubpriv_colleges.py
        self.n_pub = 470
        self.n_priv = 832

    def mapper(self, _, line):
        sample = uniform()
        fields = line.split(',')
        pubpriv = int(fields[2])
        if (pubpriv == 1 and
            sample <= self.expected_n_per_pubpriv/self.n_pub):
            yield ('public', line)
        elif (pubpriv == 2 and
              sample <= self.expected_n_per_pubpriv / self.n_priv):
            yield ('private', line)

    def reducer(self, college_type, values):
        total_samples = 0
        for value in values:
            total_samples +=1
            yield ('{} college: '.format(college_type), value)
        yield ('Total {} colleges sampled: '.format(college_type), total_samples)

if __name__ == '__main__':
    WeightedSample100.run()
