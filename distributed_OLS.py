# 2. Calculate the variance in electricity prices among the states.
from mrjob.job import MRJob
from sklearn.linear_model import LinearRegression
import numpy as np
from numpy.random import choice


class MrLeastSquares(MRJob):
    '''
    Run with:
    python distributed_OLS.py < Data/states.csv

    fit Population = Area * alpha + beta


    State data format: Name, Abbreviation, Area (Sq. Miles), Population

    Output:
    "Variance"	71.3836895496527
    '''
    def mapper(self, _, line):
        words = line.split(',')
        _, _, _, area, population = words
        # use int to solve serialization exception
        # https://stackoverflow.com/a/11942689/4212158
        bucket_id = int(choice(range(5)))
        yield (bucket_id, (int(area), int(population)))

    def reducer(self, bucked_id, values):
        areas, populations = zip(*values)
        print("areas ", areas)
        print("pop", populations)
        areas, populations = np.array(areas).reshape(-1, 1),\
                             np.array(populations).reshape(-1, 1),
        #areas, populations = areas.reshape(-1, 1), populations.reshape(-1, 1)
        try:
            coefficients = LinearRegression(fit_intercept=False).fit(areas, populations).coef_
        except ValueError:
            print(coefficients)

if __name__ == '__main__':
    MrLeastSquares.run()
