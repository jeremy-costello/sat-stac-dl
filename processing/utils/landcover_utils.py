from math import log2
import numpy as np


def compute_entropy(counts):
    """Compute Shannon entropy from a histogram (ignoring zeros)."""
    total = counts.sum()
    if total == 0:
        return 0.0
    probs = counts / total
    return -np.sum([p * log2(p) for p in probs if p > 0])
