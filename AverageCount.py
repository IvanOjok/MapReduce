from mrjob.job import MRJob

class AverageCount(MRJob):

    def mapper(self, _, line):
        # Split tab-separated values
        user_id, movie_id, rating, timestamp = line.strip().split('\t')
        yield movie_id, int(rating)

        
    def reducer(self, movie_id, rating):
        ratings = list(rating)
        yield movie_id, sum(ratings)/len(ratings)

if __name__ == '__main__':
    AverageCount.run()
