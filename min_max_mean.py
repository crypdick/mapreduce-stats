# 1. Calculate the largest, smallest, and average (mean) population for a state.
# Calculate the largest, smallest, and average (mean) area for a state.
from mrjob.job import MRJob

class MinMaxMeanPopulation(MRJob):

    def mapper(self, _, line):
        words = line.split(',')
        # State data is Name, Abbreviation, Area (Sq. Miles), Population
        yield ('Populations', words[-1])

    def reducer(self, key, values):
        smallest = 100000000000000
        largest = 0
        n_states = 0
        total_population = 0
        for v in values:
            if v < smallest:
                smallest = v
            if v > largest:
                largest = v
            total_population += v
            n_states += 1
        yield ("Smallest, largest, and average population of states: ",
               (smallest, largest, total_population/n_states))

if __name__ == '__main__':
    MinMaxMeanPopulation.run()
