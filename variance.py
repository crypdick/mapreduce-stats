# 2. Calculate the variance in electricity prices among the states.
from mrjob.job import MRJob


class VarianceElectricityCost(MRJob):
    '''
    Run with:
    python variance.py < Data/Electricity.csv

    Variance formula is E[X^2] - E[X]^2

    Output:
    "Variance"	6.514557862360633
    '''
    def mapper(self, _, line):
        _, cost = line.split(',')
        # Electricity : State, Price per Kilowatt Hour
        #try:
        yield ('Cost', float(cost))
        #except ValueError:  # some empty fields
         #   pass

    def reducer(self, key, values):
        n = 0
        sum_x_squared = 0
        sum_x = 0
        for v in values:
            n += 1
            sum_x += v
            sum_x_squared += v**2

        variance = sum_x_squared/n - (sum_x/n)**2 #TODO n should be squared for (E[x])^2
        yield ("Variance", variance)

if __name__ == '__main__':
    VarianceElectricityCost.run()
