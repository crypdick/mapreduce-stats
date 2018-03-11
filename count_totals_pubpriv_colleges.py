from mrjob.job import MRJob


class Count_College_Types(MRJob):
    '''
    python count_totals_pubpriv_colleges.py Data/colleges.csv

    "total public colleges:"        470
    "total private colleges:"       832
    '''

    def mapper(self, _, line):
        fields = line.split(',')
        try:
            pubpriv = int(fields[2])
        except:
            print("exception", fields)
            # used to find "Montana State University, Northern" with unexpected extra comma
        if pubpriv == 1:
            yield ('public', 1)
        else:
            yield ('private', 1)

    def reducer(self, school_type, values):
        count = 0
        for v in values:
            count += 1
        yield ('total {} colleges:'.format(school_type), count)


if __name__ == '__main__':
    result = Count_College_Types.run()
