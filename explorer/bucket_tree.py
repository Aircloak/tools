from collections import namedtuple
from . import bucket_util as bu


class BucketTree:
    def __init__(self, root_bucket_size, root_data):
        self._root_bucket_size = root_bucket_size
        self._buckets_by_level = {
            root_bucket_size: root_data
        }

    def buckets_at_level(self, level):
        return self._buckets_by_level.get(level)

    def insert_query_result(self, bucket_size, meta_data, query_result):
        '''Insert the result of a bucketed query

        :param bucket_size: The bucket size at this level
        :meta_data: A dict containing extra data about this level of buckets
        :param query_result: Should be a list of ((bucket_size, lower_bound), bucket_data) 
        '''
        assert bucket_size < self._root_bucket_size, "Can't insert a bucket level above the root"

        assert bucket_size < min(self._buckets_by_level.keys(
        )), "Inserting a bucket level above the lowest level is not yet supported"

        bl = BucketLevel(bucket_size=bucket_size,
                         meta_data=meta_data, query_result=query_result)

        self._buckets_by_level.update({bucket_size: bl})

    def get_bucket(self, bucket):
        result = None
        level = self.buckets_at_level(bucket.size)
        if level is None:
            result = self.try_smaller_buckets(bucket)
        else:
            result = level.find_bucket(bucket)

        if result is None:
            # there must be a gap in the buckets (or bucket is out of range)
            # check higher levels
            # lower levels will be empty if this level is empty
            result = self.try_larger_buckets(bucket)

        return result

    def try_larger_buckets(self, bucket):
        available_levels = [
            level for level in self._buckets_by_level.keys() if level > bucket.size]
        if len(available_levels) == 0:
            return self.root_bucket()
        else:
            max(available_levels)

    def root_bucket(self):
        return self._buckets_by_level[self._root_bucket_size]

    def try_smaller_buckets(self, bucket):
        available_levels = [
            level for level in self._buckets_by_level.keys() if level < bucket.size]
        if len(available_levels) == 0:
            return None

        smaller_buckets = None
        for bucket_size in sorted(available_levels, reverse=True):
            smaller_buckets = bucket.split(bucket_size)
            if smaller_buckets is not None:
                break

        return BucketData([self.get_bucket(b) for b in smaller_buckets])

    def synthesise_through_interpolation(self, bucket, level):
        assert level > bucket.size, "Cannot interpolate from smaller buckets. Merge instead."
        pass


class BucketLevel:
    '''Container class for buckets of the same size
    '''

    def __init__(self, *, bucket_size, meta_data, query_result):
        '''
        :param bucket_size: The bucket size at this level
        :param meta_data: Metadata associated with this bucket level 
        :param query_result: Should be a list of ((bucket_size, lower_bound), bucket_data) 
        '''
        self.bucket_size = bucket_size
        self.meta_data = meta_data
        self.buckets = dict([(lower_bound, Bucket(bs, lower_bound, bucket_data))
                             for ((bs, lower_bound), bucket_data) in query_result if bs == bucket_size])

    def find_bucket(self, bucket_size, lower_bound):
        if bucket_size != self.bucket_size:
            return None

        return self.buckets.get(lower_bound)

    def buckets_in_range(self, range_lo, range_hi):
        return (bucket for (lower_bound, bucket) in self.buckets
                if lower_bound >= range_lo and lower_bound < range_hi)


class Bucket:
    '''Container class for bucketed data
    '''

    def __init__(self, bucket_size, lower_bound, bucket_data=None):
        self.size = bucket_size
        self.lower_bound = lower_bound
        self.data = bucket_data

    def __eq__(self, other):
        return self.size == other.size and self.lower_bound == other.lower_bound

    def __hash__(self):
        return hash((self.size, self.lower_bound))

    def __str__(self):
        return f'Bucket({self.lower_bound} - {self.lower_bound + self.size})'

    def children(self):
        '''Split the bucket into its child buckets at the next level

        Levels progress depending on the base of the bucket size:
        Base 1 -> Base 5
        Base 2 -> Base 1
        Base 5 -> Base 1

        For example: 2000 -> 1000 -> 500 -> 100 -> 50 -> 10 -> 5 -> 1
        '''
        base = bu.base(self.size)
        if base == 1:
            children_size = self.size / 2
        elif base == 2:
            children_size = self.size / 2
        elif base == 5:
            children_size = self.size / 5
        else:
            raise ValueError(
                f'Invalid bucket size for splitting: {self.size}.')

        return self.split_to_size(children_size)

    def split_to_size(self, smaller_size):
        '''Split a bucket into multiple buckets of a smaller size
        '''
        if smaller_size >= self.size:
            raise ValueError(
                "Can't split a bucket into a larger bucket size")

        if self.size % smaller_size != 0:
            # return None to signal that the desired bucket size doesn't fit
            return None

        upper_bound = self.lower_bound + self.size
        return [Bucket(smaller_size, lower_bound) for lower_bound in range(lower_bound, upper_bound, smaller_size)]


class BucketData:
    def __init__(self, bucket_data):
        if type(bucket_data) == type([]):
            self.count = sum([data['count'] for data in bucket_data])
            self.min = min([data['min'] for data in bucket_data])
            self.max = max([data['max'] for data in bucket_data])
        else:
            self.count = bucket_data['count']
            self.min = bucket_data['min']
            self.max = bucket_data['max']

    def __str__(self):
        return f'BucketData(count: {self.count}, min: {self.min}, max: {self.max})'


def interpolate(big_bucket_data: (Bucket, BucketData),
                small_bucket_size,
                small_buckets_data: list((Bucket, BucketData))):
    '''Interpolate small buckets from a larger one. 
    '''
    if big_bucket_data[0].size % small_bucket_size != 0:
        # Small buckets don't fit nicely into big bucket.
        return []
    known_count = 0
    missing_buckets = []
    expected_lower_bound = big_bucket_data[0].lower_bound
    big_bucket_upper_bound = big_bucket_data[0].lower_bound + \
        big_bucket_data[0].size
    small_buckets_data = dict(small_buckets_data)

    while expected_lower_bound < big_bucket_upper_bound:
        expected_small_bucket = Bucket(small_bucket_size, expected_lower_bound)
        existing_data = small_buckets_data.get(expected_small_bucket)
        if existing_data is not None:
            known_count += existing_data.count
        else:
            missing_buckets.append(expected_small_bucket)
        expected_lower_bound += small_bucket_size

    unknown_count = big_bucket_data[1].count - known_count
    count_per_bucket = unknown_count / len(missing_buckets)

    synthetic_bucket_data = [(b, BucketData(
        {'count': count_per_bucket, 'min': None, 'max': None})) for b in missing_buckets]

    all_small_buckets = [*small_buckets_data.items(), *synthetic_bucket_data]
    all_small_buckets.sort(key=lambda tup: tup[0].lower_bound)
    return all_small_buckets
