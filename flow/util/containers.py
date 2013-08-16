from itertools import izip_longest


# Taken from StackOverflow
def grouper(n, iterable, padvalue=None):
    "grouper(3, 'abcdefg', 'x') --> ('a','b','c'), ('d','e','f'), ('g','x','x')"
    return izip_longest(*[iter(iterable)]*n, fillvalue=padvalue)


def head(iterable):
    return iter(iterable).next()
