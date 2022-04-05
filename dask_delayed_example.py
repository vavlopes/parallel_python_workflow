import dask

@dask.delayed
def inc(x):
    return x + 1

@dask.delayed
def double(x):
    return x * 2

@dask.delayed
def add(x, y):
    return x + y

data = [1, 2, 3, 4, 5]

output = []
for x in data:
    a = inc(x)
    b = double(x)
    c = add(a, b)
    output.append(c)

print(dask.compute(*output,num_workers = 3))    
total = dask.delayed(sum)(output)
print(dask.compute(total, num_workers = 2))    
