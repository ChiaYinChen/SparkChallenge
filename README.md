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

# 7. Go dutch!
Your friend and you went to a high class restaurant to eat dinner. After eating a cake, a waiter came and gave you the bill. You have to check the bill what are your orders and what not.

#### answer
```python
rddA = sc.parallelize([('fruit','apple'),
                       ('fruit','apple'),
                       ('fruit','banana'),
                       ('3c','mac')])

rddB = sc.parallelize([('apple', 5),
                       ('banana', 3),
                       ('kiwi', 10)])

def answer(rddA, rddB):
    # Start to write your code
    ans = rddA.map(lambda x:(x[1],x[0])) \
              .leftOuterJoin(rddB.map(lambda x:(x[0],x[1]))) \
              .collect()
    # End
    return sorted(ans)

answer(rddA=rddA, rddB=rddB)
```

#### output
```
[('apple', ('fruit', 5)),
 ('apple', ('fruit', 5)),
 ('banana', ('fruit', 3)),
 ('mac', ('3c', None))]
```

# 8. A fruit store
There is a fruit store in the corner around your company. You would like to buy some fruit for your family, so you walked into the store and bought fruit. Weight of each fruit are not equal, and you have to find the average of it.

#### answer
```python
inputA = sc.parallelize([('apple', [3, 5]), ('banana', [5, 5])])

def answer(rdd):
    # Please write your code below.
    ans = inputA.mapValues(lambda x: sum(x) / len(x)).collect()
    return ans

answer(rdd=inputA)
```

#### output
```
[('apple', 4.0), ('banana', 5.0)]
```

# 9. Happy Trip
You and your friend go travel this week. You buy flight tickets together and the format of flight ticket is ("Seat", ("your name", "id of this oder")). You have to find where is your seat from tickets.

#### answer
```python
rdd = sc.parallelize([(u'Some1', (u'ABC', 9989)),
                      (u'Some2', (u'XYZ', 235)),
                      (u'Some3', (u'BBB', 5379)),
                      (u'Some4', (u'ABC', 5379))]) 
keyword = 'ABC'

def answer(rdd, keyword):
    # Write your code below.
    ans = rdd.filter(lambda x: x[1][0] == keyword).collect()
    return ans

answer(rdd=rdd, keyword=keyword)
```

#### output
```
[('Some1', ('ABC', 9989)), ('Some4', ('ABC', 5379))]
```

# 10. Happy Trip 2
Your friend would like to know how much of the sum of order id. Please tell him/ her.

#### answer
```python
rdd =  sc.parallelize([(u'Some1', (u'ABC', 9987)),
                       (u'Some2', (u'XYZ', 235)),
                       (u'Some3', (u'BBB', 5379)),
                       (u'Some4', (u'ABC', 5379))]) 
keyword = 'ABC'

def answer(rdd, keyword):
    # Write your code below.
    ans = rdd.filter(lambda x: x[1][0] == keyword) \
             .map(lambda x: x[1][1]).sum()
    return ans

answer(rdd=rdd, keyword=keyword)
```

#### output
```
15366
```

# 11. Pick up peanuts by human intelligence
In a peanuts factory, there are many workers examining peanuts if meet the standard. If you were one of them, which peanuts will be out?

#### answer
```python
rdd = sc.parallelize([('Ryan', (1, 3, 5, 7, 9)), ('IFeng', (2, 4, 6, 8, 10))])
threshold = 3

def answer(rdd, threshold):
    # Please write your code below.
    ans = rdd.mapValues(lambda x: tuple(i for i in x if i > threshold)) \
             .collect()
    return ans

answer(rdd=rdd, threshold=threshold)
```

#### output
```
[('Ryan', (5, 7, 9)), ('IFeng', (4, 6, 8, 10))]
```

# 12. A bored child
A child was in a office for waiting his father. He found there is a word puzzle on the ground, and he tried to complete it. Please give him a little hit to solve the problem.

#### answer
```python
rdd = sc.parallelize([(2, 'hello hi how are you')])

def answer(rdd):
    # Write your code below.
    ans = rdd.mapValues(lambda x: x.split(' ')) \
             .flatMapValues(lambda x: x) \
             .collect()
    return ans

answer(rdd=rdd)
```

#### output
```
[(2, 'hello'), (2, 'hi'), (2, 'how'), (2, 'are'), (2, 'you')]
```

# 13. Do you have ever heard user defined function?
Do you have freestyle? Even if there is no function you would like to use in pyspark, you can write it by yourself! Please write your own function to do the below task.

#### answer
```python
class Ans(object):
    def answer(self, rdd):
        # Write your code below.
        ans = rdd.mapValues(lambda x: func(x)).collect()
        return ans

    @staticmethod
    def func(x):
        # Write your function below.
        y = x + 1
        return y

rdd = sc.parallelize([('apple', 5)])
ans = Ans()
ans.answer(rdd=rdd)
```

#### output
```
[('apple', 6)]
```

# 14. A hard-work clerk
David is a hard-work in BigCamera. He have to make an inventory of stock. Please help him as soon as possible!

#### answer
```python
rdd = sc.parallelize([('apple', 'fruit'), ('apple', 'fruit'), ('banana', 'fruit'), ('mac', '3c'), ('ipad', '3c'), ('ipad', '3c'), ('ipad', '3c')])

def answer(rdd):
    # Write your code below.
    ans = sorted([(i[0][1],i[0][0],i[1]) for i in rdd.countByValue().items()])
    return ans

answer(rdd=rdd)
```

#### output
```
[('3c', 'ipad', 3),
 ('3c', 'mac', 1),
 ('fruit', 'apple', 2),
 ('fruit', 'banana', 1)]
```

# 15. Find the word
Sometimes the server will receive abnormal information because of delay or some other issues. Please help me find the word/ words according to the condition.

#### answer
```python
rdd = sc.parallelize(["hello hello world"])
condition = 2

def answer(rdd, condition):
    # Start to write your code
    ans = rdd.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b) \
             .filter(lambda x: x[1] == condition) \
             .keys().collect()
    # End
    return ans

answer(rdd=rdd, condition=condition)
```

#### output
```
['hello']
```

# 16. Do you have freestyle 2?
Please find how many kinds of food were eaten by Bryan each day.

#### answer
```python
rdd = sc.parallelize([('9/30', ['egg', 'steak'])])

def answer(rdd):
    # Start to write your code
    ans = rdd.mapValues(len).collect()
    # End
    return ans

answer(rdd=rdd)
```

#### output
```
[('9/30', 2)]
```

# 17. Basic text mining
You are a expert of text mining. You would like to order your result before publishing. Please order words by count in descending and order them by alpha-beta if the number of them are the same.

#### answer
```python
rdd = sc.parallelize(["hello hello world"])

def answer(rdd):
    # Please write your code below.
    ans = rdd.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b) \
             .sortByKey(ascending=True) \
             .sortBy(lambda x: x[1], ascending=False) \
             .collect()
    return ans

answer(rdd=rdd)
```

#### output
```
[('hello', 2), ('world', 1)]
```

# 18. Dating Party
In a dating party Ryan and his friends meet Iris and her friends. They quickly found their initial are the same, so they would like to dance with people whose initial letter is the same as heself/ herself.

#### answer
```python
inputA = sc.parallelize(['Ryan', 'IFeng', 'Larry'])
inputB = sc.parallelize(['Iris', 'Rose', 'Lion'])

def answer(rdd1, rdd2):
    # Please write your code below.
    ans = rdd1.map(lambda x: (x[0],x)) \
              .union(rdd2.map(lambda x: (x[0],x))) \
              .reduceByKey(lambda x,y : (x, y)) \
              .map(lambda x: x[1]) \
              .collect()
    return ans

answer(rdd1=inputA, rdd2=inputB)
```

#### output
```
[('Ryan', 'Rose'), ('Larry', 'Lion'), ('IFeng', 'Iris')]
```

# 19. Report Number
You are a squad leader army. In one day morning, you ask your solder line up by their alpha-beta of initial letter of name and report their number. Please check if their is anyone on the wrong position.

#### answer
```python
inputA = sc.parallelize(['Ryan', 'IFeng', 'Larry'])

def answer(rdd):
    # Please write your code below.
    ans = rdd.sortByKey(ascending=True)\
             .zipWithIndex() \
             .map(lambda x: (x[1]+1, x[0])) \
             .collect()
    return ans

answer(rdd=inputA)
```

#### output
```
[(1, 'IFeng'), (2, 'Larry'), (3, 'Ryan')]
```




