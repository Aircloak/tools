
class BucketTree:
    def __init__(self, top_level_stats):
        self.top_level_stats = top_level_stats
        self.buckets_by_level = {}

    def add_level(self, bucket_size, data):
        bl = BucketLevel(bucket_size=bucket_size, **data)
        self.buckets_by_level.update({bucket_size: bl})

    def get_bucket(self, bucket):
        bucket = None
        level = self.buckets_at_level(bucket.size)
        if level is None:
            bucket = self.try_smaller_buckets(bucket)
        else:
            bucket = level.find_bucket(bucket)

        if bucket is None:
            # there must be a gap in the buckets (or bucket is out of range)
            # check higher levels
            # lower levels will be empty if this level is empty
            bucket = self.try_larger_buckets(bucket)

        return bucket

    def try_larger_buckets(self, bucket):
        available_levels = [
            level for level in self.buckets_by_level.keys() if level > bucket.size]
        if len(available_levels) == 0:
            return self.top_level_stats
        else:
            max(available_levels)

    def try_smaller_buckets(self, bucket):
        available_levels = [
            level for level in self.buckets_by_level.keys() if level < bucket.size]
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

    def buckets_at_level(self, level):
        return self.buckets_by_level.get(level)


class BucketLevel:
    def __init__(self, *, bucket_size, suppressed_count, buckets):
        self.bucket_size = bucket_size
        self.suppressed_count = suppressed_count
        self.buckets = dict([(bucket.lower_bound, bucket.data)
                             for bucket in buckets])

    def find_bucket(self, bucket_size, lower_bound):
        if bucket_size != self.bucket_size:
            return None

        return self.buckets.get(lower_bound)


class Bucket:
    def __init__(self, bucket_size, lower_bound):
        self.size = bucket_size
        self.lower_bound = lower_bound

    def __eq__(self, other):
        return self.size == other.size and self.lower_bound == other.lower_bound

    def __hash__(self):
        return hash((self.size, self.lower_bound))

    def __str__(self):
        return f'Bucket({self.lower_bound} - {self.lower_bound + self.size})'

    def split(self, smaller_size):
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


def interpolate(big_bucket_data: (Bucket, BucketData), small_bucket_size, small_buckets_data: list((Bucket, BucketData))):
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
