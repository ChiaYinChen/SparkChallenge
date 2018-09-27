# Spark Challenge
https://www.sparktw.ml/challenges/

# 1. Where is the fruit?
Please help Ryan find all fruits in the supermarket, and tell him what its name.

#### answer
```python
rdd = sc.parallelize([('apple', 'fruit'), ('apple', 'fruit'), ('banana', 'fruit'), ('mac', '3c')])
condition = 'fruit'

def answer(rdd, condition):
    # Start to write your code
    ans = rdd.filter(lambda x: x[1] == condition) \
             .distinct().keys() \
             .collect()
    # End
    return ans

answer(rdd=rdd, condition=condition)
```

#### output
```
['apple', 'banana']
```

# 2. A lonely cashier in a super market
You are a cashier in a super market. Please calculate the total cost of customers. If someone buy 5 apples one of which is 10 dollars, the cash register will show you `("apple", 10, 5)`

#### answer
```python
rdd = sc.parallelize([('apple', 10, 3), ('banana', 5, 10), ('mac', 10000, 5)])

def answer(rdd):
    # Start to write your code
    ans = rdd.map(lambda x: x[1] * x[2]).sum()
    # End
    return ans

answer(rdd=rdd)
```

#### output
```
50080
```

# 3. How to distribute properties?
If you have 500 sheep and one day you break up with your wife, you have to give her some sheep. How to distribute them ?

p.s. I don't care the order of sheep, and a approximate number of sheep will be fine.

#### answer
```python
inputA = sc.parallelize(range(1, 501)) 
your_part = 3
her_part = 2

def answer(rdd, n, m):
    # Start to write your code
    your = your_part/(your_part + her_part)
    her = 1 - your
    rdd1, rdd2 = rdd.randomSplit(weights=[your, her])
    # End
    return rdd1, rdd2

list_of_your_sheep, her_sheep = answer(rdd=inputA, n=your_part, m=her_part)
print(list_of_your_sheep.collect())
print(her_sheep.collect())
print((list_of_your_sheep.count(), her_sheep.count()))
```

#### output
```
list_of_your_sheep = sc.parallelize([1,2,3,4,5,...3xx]) 
her_sheep = sc.parallelize([3xx+1....500])
```
# 4. How much is it?
One day, Ryan went to a supermarket and buy some fruits. He took fruits to a self-accounting machine, If you are the designer of that machine, how do you design the logic of accounting?

#### answer
```python
rdd = sc.parallelize([('9/1', 'apple'), ('9/2','apple'), ('9/2','banana'),
                      ('9/3', 'apple'), ('9/3', 'mac')])
mapper = {'apple': 5, 'banana': 3, 'mac': 1000}

def answer(rdd, mapper):
    # Start to write your code.
    ans = rdd.map(lambda x: x + (mapper.get(x[1]),)) \
             .map(lambda x: (x[0],x[2])) \
             .reduceByKey(lambda x,y : x+y) \
             .collect()
    # The end of your code.
    return ans

answer(rdd=rdd, mapper=mapper)
```

#### output
```
[('9/1', 5), ('9/2', 8), ('9/3', 1005)]
```

# 5. A busy householder
A householder have to record daily cost. He is too busy to calculate the cost of this month. Please tell him how much does this family spend in this month.

#### answer
```python
rddA = sc.parallelize([('9/1', 'banana')])
rddB = sc.parallelize([('9/2','apple'), ('9/2','banana')])
rddC = sc.parallelize([('9/3', 'apple'), ('9/3', 'mac')])

def answer(rdd1, rdd2, rdd3):
    # Start to write your code
    ans = rdd1.union(rdd2).union(rdd3) \
              .map(lambda x: x[1]) \
              .countByValue().items()
    # End
    return sorted(ans)

answer(rdd1=rddA, rdd2=rddB, rdd3=rddC)
```

#### output
```
[('apple', 2), ('banana', 2), ('mac', 1)]
```

# 6. A tired programmer
Bryan didn't sleep well last night, so he can not see the monitor very clearly. Please help him connect of those two tags.

#### answer
```python
rddA = sc.parallelize([('fruit','orange'),
                       ('fruit','orange'),
                       ('fruit','banana'),
                       ('3c','mac')])

rddB = sc.parallelize([('orange', 5),
                       ('banana', 3),
                       ('kiwi', 10)])

def answer(rddA, rddB):
    # Start to write your code
    ans = rddA.map(lambda x:(x[1],x[0])) \
              .join(rddB.map(lambda x:(x[0],x[1]))) \
              .collect()
    # End
    return ans

answer(rddA=rddA, rddB=rddB)
```

#### output
```
[('orange', ('fruit', 5)), ('orange', ('fruit', 5)), ('banana', ('fruit', 3))]
```



