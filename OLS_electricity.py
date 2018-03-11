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
    area_model_prices = []
    area_model_predictors = []
    population_model_prices = []
    population_model_predictors = []


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
            yield (state, (int(area), int(population)))

        elif len(words) == 2:  # reading from electricity.csv
            _, price = words
            yield (state, float(price))

    def reducer_join_data(self, state_name, data):
        # split OLS onto various reducers
        bucket_id = self.state_to_bucket[state_name]

        for vals in data:
            if isinstance(vals, float):
                price = vals
                yield (
                    "population_model" + bucket_id,
                    (state_name, "price", price))
                yield ("area_model" + bucket_id, (state_name, "price", price))
            # lesson learned: MrJob turns tuples into lists...
            elif isinstance(vals, list):
                area, population = vals
                yield ("population_model" + bucket_id,
                       (state_name, "predictor", population))
                yield (
                    "area_model" + bucket_id, (state_name, "predictor", area))
            else:
                print(vals, type(vals))
                raise Exception

    def regression_reducer(self, bucket_id, values):
        data = {}
        model_type = bucket_id[:-1]  # drop number

        for tup in values:
            state_name = tup[0]

            if state_name not in data:
                data[state_name] = {'price': None,
                                    'predictor': None}

            if tup[1] == 'price':
                data[state_name]['price'] = tup[2]
            elif tup[1] == 'predictor':
                data[state_name]['predictor'] = tup[2]

        prices = []
        predictor = []
        for _, dicti in data.items():
            for k, v in dicti.items():
                if k == 'price':
                    prices.append(v)
                else:
                    predictor.append(v)

        # store data for scoring later
        if model_type == 'area_model':
            self.area_model_prices.extend(prices)
            self.area_model_predictors.extend(predictor)
            print('area saved')
        elif model_type == 'population_model':
            self.population_model_predictors.extend(predictor)
            self.population_model_prices.extend(prices)
            print('population data saved')
        else:
            raise Exception

        n_samples_this_reducer = len(prices)

        # make compatible shape for scikit
        predictor, prices = self.fix_list(predictor), self.fix_list(prices)
        lin_model = LinearRegression(fit_intercept=True).fit(predictor, prices)
        alpha = lin_model.coef_[0][0]  # extract alpha from array
        intercept = lin_model.intercept_[0]
        yield (model_type, ([alpha, intercept], n_samples_this_reducer))

    def average_coeffs_reducer(self, model_type, values):
        coefficients = []
        num_samples = []
        for (coeff, n_samp) in values:
            coefficients.append(coeff)
            num_samples.append(n_samp)
        coeff_weighted_avg = np.average(coefficients, weights=num_samples,
                                        axis=0)

        combined_lm = LinearRegression(fit_intercept=True)

        # coef and intercept are stored seperately
        combined_lm.coef_ = coeff_weighted_avg[0]  # this may or may not be cheating
        combined_lm.intercept_ = coeff_weighted_avg[1]

        if model_type == 'population_model':
            x = self.population_model_predictors
            y = self.population_model_prices
        elif model_type == 'area_model':
            x = self.area_model_predictors
            y = self.area_model_prices
        score = combined_lm.score(self.fix_list(x),self.fix_list(y))



        # convert to list to get around np.array JSON serialization exception
        yield ("R^2 for {} model: ".format(model_type), score)

    def fix_list(self, my_list):
        '''
        converts lists into a format which scipy linearRegression model can
        understand
        '''
        return np.array(my_list).reshape(-1, 1)


if __name__ == '__main__':
    MrLeastSquares.run()
