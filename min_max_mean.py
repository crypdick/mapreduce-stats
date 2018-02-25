# 1. Calculate the largest, smallest, and average (mean) population for a state.
# Calculate the largest, smallest, and average (mean) area for a state.
from mrjob.job import MRJob


class MinMaxMeanPopulation(MRJob):
    '''
    Run with:
    python min_max_mean.py < Data/states.csv

    Output:
   "Smallest, largest, and average population of states: "	[453588, 248691873, 9565245.115384616]
    '''
    def mapper(self, _, line):
        words = line.split(',')
        # State data is Name, Abbreviation, Area (Sq. Miles), Population
        try:
            yield ('Populations', int(words[-1]))
        except ValueError:  # some empty fields
            pass

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
               (smallest, largest, total_population / n_states))


class MinMaxMeanArea(MRJob):
    '''
    Run with:
    python min_max_mean.py < Data/states.csv

    Output:
   "Smallest, largest, and average area of states: "  [68, 3787316, 145666.0]
    '''
    def mapper(self, _, line):
        words = line.split(',')
        # State data is Name, Abbreviation, Area (Sq. Miles), Population
        try:
            yield ('Area', int(words[-2]))
        except ValueError:  # some empty fields
            pass

    def reducer(self, key, values):
        smallest = 100000000000000
        largest = 0
        n_states = 0
        total_area = 0
        for v in values:
            if v < smallest:
                smallest = v
            if v > largest:
                largest = v
            total_area += v
            n_states += 1
        yield ("Smallest, largest, and average area of states: ",
               (smallest, largest, total_area / n_states))


if __name__ == '__main__':
    #MinMaxMeanPopulation.run()
    # TODO: figure out how to reuse same class but specify different index in list
    MinMaxMeanArea.run()
