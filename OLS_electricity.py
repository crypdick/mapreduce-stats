from mrjob.job import MRJob
from mrjob.step import MRStep
from sklearn.linear_model import LinearRegression
import numpy as np
from numpy.random import choice


class MrLeastSquares(MRJob):
    '''
    Run with:
    python OLS_electricity.py < Data/states.csv

    Which of the following linear models is a better fit for the electricity data

    Electricity Price = Area * alpha + beta

    Or

    Electricity Price = Population * alpha + beta

    Output:
    "Weighted average of linear regression coefficients: "	[-4.921647793626868, 5496240.323258571]
    '''
    state_to_bucket = {}

    def steps(self):
        return [MRStep(
                       mapper=self.mapper_parse_csvs,
                       reducer=self.reducer_join_data),
                MRStep(reducer=self.regression_reducer),
                MRStep(reducer=self.average_coeffs_reducer)]

    def mapper_parse_csvs(self, _, line):
        words = line.split(',')

        state = words[0]
        if state not in self.state_to_bucket:
            bucket_id = str(choice(range(5)))
            self.state_to_bucket[state] = bucket_id

        if len(words) == 5:  # reading from states.csv
            _, _, _, area, population = words
            print(state, area, population)
            yield (state, tuple(int(area), int(population)))
            # use int to solve serialization exception
            # https://stackoverflow.com/a/11942689/4212158
            #bucket_id = int(choice(range(5)))
            #try:
             #   yield (bucket_id, (int(area), int(population)))
        elif len(words) == 2:  # reading from electricity.csv
            _, price = words
            print(state, price)
            yield (state, float(price))

    def reducer_join_data(self, state_name, data):
        bucket_id = self.state_to_bucket[state_name]

        for vals in data:
            #price, area, population = None, None, None
            if isinstance(vals, float):
                price = vals
                yield ("population_model" + bucket_id, ("price", price))
                yield ("area_model" + bucket_id, ("price", price))
            elif isinstance(vals, tuple):
                yield ("population_model" + bucket_id, ("area + pop", vals))
                yield ("area_model" + bucket_id, ("area + pop", vals))
            else:
                print(vals, type(vals))
                raise Exception




    def regression_reducer(self, bucket_id, values):
        for v in values:

        model_type = bucket_id[:-1]  # drop number
        predictor, price = zip(*values)
        n_samples_this_reducer = len(price)
        # make compatible shape for scikit
        predictor, price = np.array(predictor).reshape(-1, 1),\
                             np.array(price).reshape(-1, 1),
        lin_model = LinearRegression(fit_intercept=True).fit(predictor, price)
        alpha = lin_model.coef_[0][0]  # extract alpha from array
        intercept = lin_model.intercept_[0]
        yield (model_type, ([alpha, intercept], n_samples_this_reducer))


    def average_coeffs_reducer(self, model_type, values):
        coefficients = []
        num_samples = []
        for (coeff, n_samp) in values:
            coefficients.append(coeff)
            num_samples.append(n_samp)
        coeff_weighted_avg = np.average(coefficients, weights=num_samples, axis=0)

        # convert to list to get around np.array JSON serialization exception
        yield ("Weighted average of linear regression coefficients \n "
               "for model {}: ".format(model_type), list(coeff_weighted_avg))


if __name__ == '__main__':
    MrLeastSquares.run()
