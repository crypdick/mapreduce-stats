from mrjob.job import MRJob
from mrjob.step import MRStep
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
    "Weighted average of linear regression coefficients: "	[-4.921647793626868, 5496240.323258571]
    '''

    def steps(self):
        return [MRStep(mapper=self.mapper,
                       reducer=self.regression_reducer),
                MRStep(reducer=self.average_coeffs_reducer)]

    def mapper(self, _, line):
        words = line.split(',')
        _state, _, _, area, population = words
        # use int to solve serialization exception
        # https://stackoverflow.com/a/11942689/4212158
        bucket_id = int(choice(range(5)))
        try:
            yield (bucket_id, (int(area), int(population)))
        except ValueError:
            # there are some bs entries at the tail of the csv
            # solve by deleting them
            print(_state, area, population)

    def regression_reducer(self, bucket_id, values):
        areas, populations = zip(*values)
        n_samples_this_reducer = len(areas)
        # make compatible shape for scikit
        areas, populations = np.array(areas).reshape(-1, 1),\
                             np.array(populations).reshape(-1, 1),
        lin_model = LinearRegression(fit_intercept=True).fit(areas, populations)
        alpha = lin_model.coef_[0][0]  # extract alpha from array
        intercept = lin_model.intercept_[0]
        yield ('_', ([alpha, intercept], n_samples_this_reducer))

    def average_coeffs_reducer(self, _, values):
        coefficients = []
        num_samples = []
        for (coeff, n_samp) in values:
            coefficients.append(coeff)
            num_samples.append(n_samp)
        coeff_weighted_avg = np.average(coefficients, weights=num_samples, axis=0)

        # convert to list to get around np.array JSON serialization exception
        yield ("Weighted average of linear regression coefficients: ", list(coeff_weighted_avg))


if __name__ == '__main__':
    MrLeastSquares.run()
