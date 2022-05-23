from collections import deque
from itertools import cycle, islice, repeat
from typing import Any, Dict, Generator, List, Tuple, Union

Pair = Tuple[Any, Any]
Record = Dict[Any, Any]

def dict_roundrobin(d: Dict[Any, List[Any]], orient: str) -> Union[Generator[Pair, None, None], Generator[Record, None, None]]:
    """
    Return an iterable yielding column value pairs from a dictionary of lists. The iterable can
    return pairs as a generator of (key, value) tuples, or a generator of dicts with only
    non-exhausted key-value pairs.

    parameters:
        d (dict[Any, list[Any]]): dictionary of {key: [value]}
        orient str: "pairs" or "records"
            `"pairs"` returns a generator of (key, value) tuples (`Record`s)
            `"records"` returns a generator of sub dict records with non-exhausted key-value pairs (`Record`s)

    Examples:
    >>> data = {
            'Col_1': ['1a', '1b', '1c'],
            'Col_2': ['2a', '2b'],
            'Col_3': [3.0]
        }
    >>> list(dict_roundrobin(data, orient="records"))
    >>> [
            {
                'Col_1': '1a',
                'Col_2': '2a',
                'Col_3': 3.0
            },
            {
                'Col_1': '1a',
                'Col_2': '2a'
            },
            {
                'Col_1': 1
            }
        ]

    >>> list(dict_roundrobin(data, orient="pairs"))
        [
            ('Col_1', 1),
            ('Col_2', 2),
            ('Col_3', 3.0),
            ('Col_1', 1),
            ('Col_2', 2),
            ('Col_1', 1)
        ]
    """
    if not isinstance(d, dict):
        raise ValueError(f"dict_roundrobin(): `d` must be a dictionary of lists. `d` was of non-dictionary type: {type(d)}.")
    
    if not all(isinstance(v, list) for v in d.values()):
        bad_kvs = "\n".join(f"{k}: {type(v)}" for k, v in d.items() if not isinstance(v, list))
        raise ValueError(f"dict_roundrobin(): `d` must be a dictionary of lists. Non list records found:\n{bad_kvs}.")

    num_active = len(d.keys())
    nexts = cycle(iter(it).__next__ for it in [zip(repeat(k), v) for k, v in d.items()])

    if orient == "pairs":
        d = deque(maxlen=0)
        pairs = True
    elif orient == "records":
        d = deque(maxlen=num_active)
        pairs = False
    else:
        raise ValueError(f"orient must be `'records'` or `'pairs'` '{orient}' is not valid.")

    while num_active:
        try:
            for next in nexts:
                if pairs:
                    yield next()
                else:
                    if len(d) == num_active:
                        yield dict(list(d))
                        d.clear()
                    d.append(next())
        except StopIteration:
            # Remove the iterator we just exhausted from the cycle.
            num_active -= 1
            nexts = cycle(islice(nexts, num_active))


def main() -> None:
    import json # only used for pretty printing when running locally

    data = {
        'Col_1': ['1a', '1b', '1c'],
        'Col_2': ['2a', '2b'],
        'Col_3': [3.0],
    }
    
    print("dict_roundrobin1:\n-----------------")
    print(json.dumps(list(dict_roundrobin(data, orient="pairs")), indent=2))

    print()

    print("dict_roundrobin2:\n-----------------")
    print(json.dumps(list(dict_roundrobin(data, orient="records")), indent=2))

if __name__ == "__main__":
    main()
