import dask

@dask.delayed
def inc(x):
    return x + 1

@dask.delayed
def fn(x):
    return x**1000

@dask.delayed
def add(x, y):
    return x + y

data = [100000000000001, 20000, 300000, 400000, 500000]

output = []
for x in data:
    a = inc(x)
    b = fn(x)
    c = add(a, b)
    output.append(c)

# Running map instead a for
output = list(map(lambda x: add(fn(inc(x)),fn(x)), data))
[x.compute() for x in output]

total = dask.delayed(sum)(output) #delayed is applicable to a function not the result

total.compute(scheduler='processes')