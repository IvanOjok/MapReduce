from mrjob.job import MRJob

class AverageCount(MRJob):

    def mapper(self, _, line):
        # Split tab-separated values
        user_id, item_id, rating, timestamp = line.strip().split('\t')
        yield item_id, int(rating)

        
    def reducer(self, item_id, rating):
        ratings = list(rating)
        yield item_id, sum(ratings)/len(ratings)

if __name__ == '__main__':
    AverageCount.run()