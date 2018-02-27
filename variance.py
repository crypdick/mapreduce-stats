# 2. Calculate the variance in electricity prices among the states.
from mrjob.job import MRJob


class VarianceElectricityCost(MRJob):
    '''
    Run with:
    python variance.py < Data/Electricity.csv

    Variance formula is E[X^2] - E[X]^2

    Output:
    "Variance"	71.3836895496527
    '''
    def mapper(self, _, line):
        _, cost = line.split(',')
        # Electricity : State, Price per Kilowatt Hour
        #try:
        cost = float(cost)
        yield ('bucket', (cost, cost**2, 1))
        #except ValueError:  # some empty fields
         #   pass

    def combiner(self, key, values):
        n = 0
        sum_x_squared = 0
        sum_x = 0
        for cost, costsq, count in values:
            n += count
            sum_x += cost
            sum_x_squared += costsq

        yield ("bucket", (sum_x, sum_x_squared, n))

    def reducer(self, key, values):
        n = 0
        sum_x_squared = 0
        sum_x = 0
        for sum_cost, sum_costsq, count in values:
            n += count
            sum_x += sum_cost
            sum_x_squared += sum_costsq

        variance = sum_x_squared/n - (sum_x/n**2)**2
        yield ("Variance", variance)

if __name__ == '__main__':
    VarianceElectricityCost.run()
