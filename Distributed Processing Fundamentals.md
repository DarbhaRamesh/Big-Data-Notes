
Lets understand MapReduce with an example.

Lets say we have data of LinkedIn where one user viewed another users profile. For simplicity, we are taking 6 lines of data

| s.no | from user | to user |
| ---- | --------- | ------- |
| 1    | User A    | User B  |
| 2    | User B    | User C  |
| 3    | User A    | User C  |
| 4    | User D    | User B  |
| 5    | User B    | User A  |
| 6    | User E    | User B  |
Above data can be interpreted as User A visited user B's profile in LinkedIn. We want to understand how many views are there for a particular profile.

Step 1 - Record Reader Output

(1, [1,User A, User B])
(2,[2,User B, User C])
(3, [3, User A, User C])
(4, [4,User D, User B])
(5, [5, User B, User A])
(6,[6,User E, User B])

Step 2 - Mapper Output

(User B,1)
(User C, 1)
(User C, 1)
(User B,1)
(User A,1)
(User B,1)

Step 3 - Shuffle - Sort

(User A, 1)
(User B,1)
(User B,1)
(User B,1)
(User C, 1)
(User C, 1)

(User A, {1})
(User B, {1,1,1})
(User C,{1,1})

Step 4 - Reducer

(User A, 1)
(User B, 3)
(User C, 2)

How many mappers would run? - It depends on no. of blocks a file is broken down.

How many mappers would run in parallel? - It depends on the cluster configuration (mo. of data nodes)

How many reducers would run? - This developer can configure. Can be zero or more than 1 but by default it is 1.

When reducer is over burdened, then we can increase the no. of reducers to optimize your job. 
- if we have more than one reducer, then mappers output will be divided into **partitions** based on number of reducers
- There is a **hash function** which is inbuilt, will partition the data at mapper. And this will define which partition will go to which reducer. This Hash function is very consistent meaning all the same keys should go to same reducer.
- If you have to define **custom partitioning logic**, we can write your own logic but this logic should be consistent.

When you don't need to aggregate then we don't require reducer. Example - filtering data.
- if a file is 1GB and by default this is divided into 8 blocks. we will have 8 output files.

---

Another Example - 

If a sensor records temperature every hour and we have to find the max temperature day wise using MR.

data might be in below format

| date | time | temperature |
| ---- | ---- | ----------- |
| D1   | T1   | 47          |
| D1   | T2   | 49          |
| D1   | T3   | 45          |
| D2   | T1   | 39          |
| D2   | T2   | 38          |
| D2   | T3   | 40          |

Mapper output can be

M1 
(D1,47)
(D1,49)
M2
(D1,45)
(D2,39)
M3
(D2,38)
(D2,40)

At reducer, we will find the max for each day

(D1,49)
(D2, 40)

In the above case, most of the processing is done at reducer end and minimal activity at mapper end. 

If at mapper end, we have performed local aggregation which is called **combiner**, Then at reducer end to some extent we could have reduced the burden.

Instead mapper output can be
M1
(D1,49)
M2
(D1,45)
(D2,39)
M3
(D2,40)

At reducer, output can be

(D1,49)
(D2, 40)

**Combiner helps us to optimize but should be used with caution.** Because if you have to find the average, then you cant find average at each mapper instead finding sum and count from each mapper would yield correct result.

---

MapReduce code can be executed from below command

``` console
hadoop jar <jar path> <input hdfs file path> <non existing output directory>
```

