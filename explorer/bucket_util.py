import logging

# assume for now that we want at least 20 values per bucket (valid?)
# also that the smallest useful bucket size is at 1/100 of the total range
MAX_BUCKETS = 100
MIN_BUCKET_COUNT = 20

'''
A note about terminology:
Bucket size: The size of the range of values contained in a bucket
Bucket count: The number of values contained in a bucket
Bucket number: The number of buckets contained in a range

For example for 1000 values in the range 0 - 100, we may have:
Bucket size: 5
Bucket count: 50 on average
Bucket number: 20
'''
BUCKETS = sorted([base * (10 ** exponent)
                  for base in [1, 2, 5] for exponent in range(-4, 20)])


def estimate_bucket_size(value_range: float, value_count: int,
                         num_buckets=MAX_BUCKETS, min_bucket_count=MIN_BUCKET_COUNT) -> int:
    '''Estimate a suitable bucket size based on desired precision and size restrictions.

    :param value_range: The size of the value range to be bucketed.
    :param value_count: The number of values contained in the dataset.
    :param num_buckets: The desired number of buckets for sufficient precision / resolution.
    :param min_bucket_count: The lowest number of values desired in each bucket
    :returns: A suitable bucket size.count

    For example, the dataset contains 10_000 values in the range 2042 -> 5683.
    The value_range is 5683 - 2042 = 3641
    If num_buckets is 100, the average bucket size is 36.4 for an estimated bucket count
    of 100.
    At the min_bucket_count of 20 we would have 10_000 / 20 = 500 buckets of size 3641 / 500 = 7.2.
    So we would like a bucket size of at least 7.2 and we are targeting 36.4 for sufficient precision.
    In this range there are two suitable bucket sizes: 10 and 20.
    >>> Buckets().estimate_bucket_size(5683 - 2042, 10_000)
    10

    Note:
        - The returned size may not meet both of the desired criteria.
        - The min_bucket_count takes priority.
    '''
    logging.debug(
        f'''Estimating bucket size for:
                range {value_range},
                count {value_count},
                num buckets {num_buckets},
                min bucket count {min_bucket_count}''')
    # Estimate lower and upper bounds for the bucket size
    precision_bound = value_range / num_buckets
    size_bound = value_range / (value_count / min_bucket_count)

    logging.debug(
        f'Precision bound: {precision_bound}, Size bound: {size_bound}')

    bs_candidates = buckets_in_range(size_bound, precision_bound)
    logging.debug(f'Options are: {bs_candidates}')

    if len(bs_candidates) == 0:
        # No bucket sizes within the range, prioritise the size bound
        result = next_after(size_bound)
    else:
        # Otherwise choose the largest bucket size within the range
        result = max(bs_candidates)

    logging.debug(f'Returning bucket size {result}')

    return result
    # bs_candidate_lower = self.next_after(lower)
    # bs_candidate_upper = self.first_before(upper)
    # if bs_candidate_lower == bs_candidate_upper:
    #     # There is only one bucket size that falls within the desired range
    #     return bs_candidate_lower
    # else:
    #     # If both estimates fall outside the intended range, choose estimate
    #     # based on the lower bound
    #     return bs_candidate_lower
    #     # Otherwise if the lower estimate is within the bounds, choose it
    #     elif bs_candidate_lower < upper:
    #         return bs_candidate_lower
    #     # Otherwise check that the upper estimate is within bounds, if so, choose it
    #     elif bs_candidate_upper > lower:
    #         return bs_candidate_upper
    #     # If none of these conditions apply, something has gone wrong...
    #     else:
    #         logging.error(
    #             f'Unable to estimate bucket size for range {lower} -> {upper}')
    #         return 0


def next_after(val):
    return next(v for v in BUCKETS if v > val)


def first_before(val):
    return next(v for v in reversed(BUCKETS) if v < val)


def buckets_smaller_than(val):
    return (v for v in reversed(BUCKETS) if v < val)


def buckets_larger_than(val):
    return (v for v in BUCKETS if v > val)


def buckets_in_range(lo, hi):
    smaller_than_hi = set(buckets_smaller_than(hi))
    larger_than_lo = set(buckets_larger_than(lo))
    return list(smaller_than_hi & larger_than_lo)


def buckets_with_base(bases):
    return (v for v in BUCKETS if base(v) in bases)


def base(val):
    base = val
    while base >= 10:
        base //= 10
    while base < 1:
        base *= 10
    return int(base)


if __name__ == "__main__":
    import doctest
    doctest.testmod()
