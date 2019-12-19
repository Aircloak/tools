from typing import List, Generator, Iterable
from collections import namedtuple
from . import bucket_util as bu
from itertools import takewhile, dropwhile


'''TREE_BASES determines the bucket sizes that are used to build the tree. 
[1, 5] means that base-1 and base-5 buckets are used, eg: 500->100->50->10 etc
'''
TREE_BASES = [1, 5]


class BucketTree:
    def __init__(self, unbucketed_range, unbucketed_data, total_count, suppressed_count):
        self._unbucketed = {
            'range': unbucketed_range,
            'data': unbucketed_data,
            'suppressed': suppressed_count
        }
        first_bucket = bu.estimate_bucket_size(unbucketed_range, total_count)
        self._to_explore = [b for b in bu.buckets_with_base(
            TREE_BASES) if b < first_bucket]

        self._to_explore.append(first_bucket)
        self._explored_buckets = {}

    def next_levels(self, depth):
        return self._to_explore[-depth:]

    def buckets_at_level(self, level):
        return self._explored_buckets.get(level).as_flat_list()

    def bucket_levels(self):
        return list(self._explored_buckets.keys())

    def insert_query_result(self, bucket_size, buckets, **kwargs):
        '''Insert the result of a bucketed query

        :param bucket_size: The bucket size at this level
        :metadata: A dict containing extra data about this level of buckets (eg. column labels, suppressed values)
        :param buckets: Should be a list of `Bucket`s
        '''
        next_level = self._to_explore.pop()
        assert bucket_size == next_level, f'Wrong bucket size, expected {next_level}, got {bucket_size}'

        metadata = dict(kwargs)
        bl = BucketLevel(bucket_size=bucket_size,
                         metadata=metadata, buckets=buckets)

        self._explored_buckets.update({bucket_size: bl})

    def get_bucket(self, bucket):
        result = None
        level = self.buckets_at_level(bucket.size)
        if level is not None:
            result = level.get_bucket(bucket)

        return result

    def get_buckets(self, levels):
        if len(levels) == 0:
            levels = self.bucket_levels()
        return [bucket for level in levels for bucket in self.buckets_at_level(level)]


class BucketLevel:
    '''Container class for buckets of the same size
    '''

    def __init__(self, *, bucket_size, buckets, metadata=None, parent_level=None):
        '''
        :param bucket_size: The bucket size at this level
        :param metadata: Metadata associated with this bucket level
        :param buckets: Should be a list of `Bucket`s
        :param parent: If the parent is not provided, fill in gaps between buckets
            with empty buckets (count = 0), otherwise interpolate missing buckets.
        '''
        self._bucket_size = bucket_size
        self._metadata = metadata
        if parent_level is None:
            fake_lo = min(bucket.lower_bound for bucket in buckets)
            fake_hi = max(bucket.upper_bound() for bucket in buckets)
            fake_count = sum(bucket.data.count for bucket in buckets)
            parent_level = [
                Bucket(fake_hi - fake_lo, fake_lo, [fake_count, fake_lo, fake_hi], FakeData)]

        interpolated = []
        bucket_iter = iter(buckets)
        for parent_bucket in parent_level:
            children = list(
                takewhile(lambda small: parent_bucket.contains(small), bucket_iter))
            interpolated += parent_bucket.interpolate_children(children)

        self._buckets = dict([(bucket.lower_bound, bucket)
                              for bucket in interpolated])

    def get_bucket(self, bucket_size, lower_bound):
        if bucket_size != self._bucket_size:
            return None

        return self._buckets.get(lower_bound)

    def buckets_in_range(self, range_lo, range_hi):
        return (bucket for (lower_bound, bucket) in self._buckets
                if lower_bound >= range_lo and lower_bound < range_hi)

    def add_metadata(self, metadata):
        self._metadata.update(metadata)

    def as_flat_list(self):
        return [bucket.flatten() for bucket in self._buckets.values()]

    def __iter__(self):
        return self._buckets.values()


QueryData = namedtuple('QueryData', 'count count_noise min max avg')
FakeData = namedtuple('FakeData', 'count min max')
SyntheticData = namedtuple('SyntheticData', 'count')
EmptyData = namedtuple('EmptyData', '')


class Bucket:
    '''Container class for bucketed data
    '''

    def __init__(self, bucket_size, lower_bound, bucket_data, data_wrapper=QueryData):
        self.size = bucket_size
        self.lower_bound = lower_bound
        if bucket_data is not None:
            self.data = data_wrapper(*bucket_data)
        else:
            self.data = EmptyData()

    def __eq__(self, other):
        return self.size == other.size and self.lower_bound == other.lower_bound

    def __hash__(self):
        return hash((self.size, self.lower_bound))

    def __str__(self):
        return f'Bucket({self.lower_bound} - {self.lower_bound + self.size})'

    def index(self):
        return (self.size, self.lower_bound)

    def upper_bound(self):
        return self.lower_bound + self.size

    def contains(self, other):
        return self.lower_bound <= other.lower_bound and self.upper_bound() >= other.upper_bound()

    def parent_index(self, parent_size):
        # TODO: add some assertions / restrictions regarding buckets sizes
        return (parent_size, (self.lower_bound // parent_size) * parent_size)

    def child_indices(self, child_size):
        # TODO: add some assertions / restrictions regarding buckets sizes
        return [(child_size, i) for i in range(self.lower_bound, self.lower_bound + self.size, child_size)]

    def flatten(self):
        return [self.size, self.lower_bound, *self.data]

    def split_to_size(self, smaller_size):
        '''Split a bucket into multiple buckets of a smaller size
        '''
        if smaller_size >= self.size:
            raise ValueError(
                "Can't split a bucket into a larger bucket size")

        if self.size % smaller_size != 0:
            # return None to signal that the desired bucket size doesn't fit
            return None

        return [Bucket(*index) for index in self.child_indices(smaller_size)]

    def interpolate_children(self, small_buckets):
        '''Interpolate gaps in small buckets from a larger one.
        '''
        small_bucket_size = small_buckets[0].size
        assert self.size % small_bucket_size == 0, f'Bucket {self.size} does not divide exactly into buckets of size {small_bucket_size}'

        small_buckets_expected_num = int(self.size // small_bucket_size)
        if small_buckets_expected_num == len(small_buckets):
            return small_buckets

        expected_lower_bounds = [self.lower_bound + i * small_bucket_size
                                 for i in range(small_buckets_expected_num)]
        provided_lower_bounds = [
            bucket.lower_bound for bucket in small_buckets]

        missing_lower_bounds = set(expected_lower_bounds) - \
            set(provided_lower_bounds)

        missing_total = self.data.count - \
            sum(bucket.data.count for bucket in small_buckets)
        count_per_bucket = missing_total / len(missing_lower_bounds)

        synthetic_buckets = [Bucket(small_bucket_size, lower_bound, [count_per_bucket], data_wrapper=SyntheticData)
                             for lower_bound in missing_lower_bounds]

        small_buckets += synthetic_buckets
        small_buckets.sort(key=lambda bucket: bucket.lower_bound)
        return small_buckets
