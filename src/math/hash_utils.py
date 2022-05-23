from typing import Any, Callable, Generator, TypeVar

T = TypeVar("T")

def hash_combine32(h1: T, h2: T) -> T:
    """ c++ boosts' hash_combine function for 32-bit hash values
    returns a mixed/combined result of the 2 (ideally) 32 bit hashed values
    """
    return h1 ^ (h2 + 0x9e3779b9 + (h1 << 6) + (h1 >> 2))

def hash_extender(hasher1: Callable[[Any], T],
                  hasher2: Callable[[Any], T],
                  val: Any) -> Generator[T, None, None]:
    """ Utility function used to approximate many hash functions from 2 strong hash
    functions. Implementation based on the paper:
    'Less Hashing, Same Performance: Building a Better Bloom Filter by Kirsch et al.'

    The paper describes a technique that only requires two independent hash functions to
    effectively implement heuristic data stuctures like bloom filters or Sketches.

    This implementation produces infinite `good` hashes by returning a generator expression.
    
    The `hasher1` and `hasher2` should both be strong non cryptographic hashing functions e.g. murmurhash3()
    
    To get exactly `n` hashes an easy approach would be to use:
    >>> `hashes = islice(hash_extender(hasher1, hasher2, val), n)`
    """
    h1 = hasher1(val)
    h2 = hasher2(val)
    yield from hash_value_extender(h1, h2, val)

def hash_value_extender(h1: T, h2: T, val: Any) -> Generator[T, None, None]:
    """ Same function as `hash_extender()`, except `hash_value_extender() expects 
    `h1` and `h2` to be values from the hash functions instead of a Callable expression.

    To get exactly `n` hashes an easy approach would be to use:
    >>> `hashes = islice(hash_value_extender(h1, h2, val), n)`
    """

    yield h1
    yield h2

    i = 0
    while True:
        yield h1 + ((i+1)*h2)
        i += 1

