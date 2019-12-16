from typing import List, Generator, Iterable
from collections import namedtuple
from . import bucket_util as bu
from itertools import takewhile, dropwhile


class BucketTree:
    def __init__(self, root_bucket_size, root_bucket):
        self._root_bucket_size = root_bucket_size
        self._buckets_by_level = {
            root_bucket_size: BucketLevel(
                bucket_size=root_bucket_size, buckets=[root_bucket])
        }

    def levels_below(self, level):
        '''For determining the next appropriate bucket size(s)
        '''
        return (bs for bs in bu.buckets_smaller_than(level) if bu.base(bs) != bu.base(self._root_bucket_size))

    def next_level(self):
        smallest_level_so_far = min(self._buckets_by_level.keys())
        return next(self.levels_below(smallest_level_so_far))

    def buckets_at_level(self, level):
        return self._buckets_by_level.get(level)

    def root_bucket(self):
        return self._buckets_by_level[self._root_bucket_size]

    def insert_query_result(self, bucket_size, metadata, buckets):
        '''Insert the result of a bucketed query

        :param bucket_size: The bucket size at this level
        :metadata: A dict containing extra data about this level of buckets (eg. column labels, suppressed values)
        :param buckets: Should be a list of (bucket_size, lower_bound, bucket_data) 
        '''
        assert bucket_size < self._root_bucket_size, "Can't insert a bucket level above the root"

        assert bucket_size < min(self._buckets_by_level.keys(
        )), "Inserting a bucket level above the lowest level is not yet supported"

        next_level = self.next_level()
        assert bucket_size == next_level, f'Wrong bucket size, expected {next_level}'

        bl = BucketLevel(bucket_size=bucket_size,
                         metadata=metadata, buckets=buckets)

        self._buckets_by_level.update({bucket_size: bl})

    def get_bucket(self, bucket):
        result = None
        level = self.buckets_at_level(bucket.size)
        if level is not None:
            result = level.get_bucket(bucket)

        return result


# TODO:
# - BucketLevel constructor to take `parent` argument
# - If `parent` is None, fill in gaps with zero-count buckets
# - Otherwise interpolate missing buckets from the parent.
class BucketLevel:
    '''Container class for buckets of the same size
    '''

    def __init__(self, *, bucket_size, buckets, metadata=None):
        '''
        :param bucket_size: The bucket size at this level
        :param metadata: Metadata associated with this bucket level 
        :param buckets: Should be a list of Bucket
        '''
        self.bucket_size = bucket_size
        self.metadata = metadata
        self.buckets = dict([(bucket.lower_bound, bucket)
                             for bucket in buckets if bucket.size == bucket_size])

    def get_bucket(self, bucket_size, lower_bound):
        if bucket_size != self.bucket_size:
            return None

        return self.buckets.get(lower_bound)

    def buckets_in_range(self, range_lo, range_hi):
        return (bucket for (lower_bound, bucket) in self.buckets
                if lower_bound >= range_lo and lower_bound < range_hi)

    def add_metadata(self, metadata):
        self.metadata.update(metadata)

    def __iter__(self):
        return self.buckets.values()


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

    def upper_bound(self):
        return self.lower_bound + self.size

    def contains(self, other: Bucket):
        return self.lower_bound >= other.lower_bound and self.upper_bound() < other.upper_bound()


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


def interpolate_children(big_bucket: Bucket,
                         small_buckets: List[Bucket]):
    '''Interpolate gaps in small buckets from a larger one. 
    '''
    small_bucket_size = small_buckets[0].size
    assert big_bucket.size % small_bucket_size == 0, f'Big bucket {big_bucket.size} does not divide exactly into small buckets of size {small_bucket_size}'

    small_buckets_expected_num = big_bucket.size / small_bucket_size
    if small_buckets_expected_num == len(small_buckets):
        return small_buckets

    expected_lower_bounds = [big_bucket.lower_bound + i * small_bucket_size
                             for i in range(small_buckets_expected_num)]
    provided_lower_bounds = [bucket.lower_bound for bucket in small_buckets]

    missing_lower_bounds = set(expected_lower_bounds) - \
        set(provided_lower_bounds)

    missing_total = big_bucket.data.count - \
        sum(bucket.data.count for bucket in small_buckets)
    count_per_bucket = missing_total / len(missing_lower_bounds)

    synthetic_buckets = [Bucket(small_bucket_size, lower_bound, {
                                'count': count_per_bucket}) for lower_bound in missing_lower_bounds]

    small_buckets += synthetic_buckets
    small_buckets.sort(key=lambda bucket: bucket.lower_bound)
    return small_buckets


def interpolate_next_level(big_buckets: BucketLevel, small_buckets_incomplete: List[Bucket]) -> BucketLevel:
    next_level = next(bu.levels_below(big_buckets.bucket_size))
    small_buckets_incomplete.sort(key=lambda bucket: bucket.lower_bound)
    completed_buckets = []
    for big_bucket in big_buckets:
        children = list(
            takewhile(lambda small: big_bucket.contains(small), small_buckets_incomplete))
        completed_buckets += interpolate_children(big_bucket, children)

    return BucketLevel()
