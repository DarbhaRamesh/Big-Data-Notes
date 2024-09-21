##### Python Essentials for Spark

Normal Function and Lambda function

``` python
# define the function
def my_sum(x,y):
	return x+y
# call the function
print(my_sum(2,3)) #output - 5
```

```python
my_list = [1,2,3,4]
list(map(lambda x: 2*x, my_list))
```

lambda function is a small anonymous function (which doesn't have name)

``` python
from functools import reduce
my_list = [1,2,4,5,6]
reduce(lambda x,y: x+y, my_list) # takes two elements in a list and sum it.
```

map and reduce are **higher order functions** as it takes another function in parameter.

Higher order functions are which takes another function as input or gives function as output.

---

