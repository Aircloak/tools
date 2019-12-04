import logging


class Buckets():
    def __init__(self):
        self.buckets = sorted([base * (10 ** exponent)
                               for base in [1, 2, 5] for exponent in range(-4, 20)])

    def estimate_bucket_size(self, lower, upper):
        # If the lower bound is higher than the upper bound, prioritise the lower bound
        # to avoid having too many useless buckets
        #
        if lower > upper:
            return self._next_after(lower)
        else:
            bs_candidate_lower = self._next_after(lower)
            bs_candidate_upper = self._first_before(upper)
            if bs_candidate_lower == bs_candidate_upper:
                return bs_candidate_lower
            else:
                # If both estimates fall outside the intended range, choose the closest
                if bs_candidate_upper < lower and bs_candidate_lower > upper:
                    diff_below = lower - bs_candidate_upper
                    diff_above = bs_candidate_lower - upper
                    if diff_below < diff_above:
                        return bs_candidate_upper
                    else:
                        return bs_candidate_lower
                # Otherwise if the lower estimate is within the bounds, choose it
                elif bs_candidate_lower < upper:
                    return bs_candidate_lower
                # Otherwise check that the upper estimate is within bounds, if so, choose it
                elif bs_candidate_upper > lower:
                    return bs_candidate_upper
                # If none of these conditions apply, something has gone wrong...
                else:
                    logging.error(
                        f'Unable to estimate bucket size for range {lower} -> {upper}')
                    return 0

    def _next_after(self, val):
        return next(v for v in self.buckets if v > val)

    def _first_before(self, val):
        return next(v for v in reversed(self.buckets) if v < val)
