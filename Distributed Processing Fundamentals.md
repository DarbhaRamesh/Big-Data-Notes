
Lets understand MapReduce with an example.

Lets say we have data of LinkedIn where one user viewed another users profile. For simplicity, we are taking 6 lines of data

| s.no | from user | to user |
| ---- | --------- | ------- |
| 1    | User A    | User B  |
| 2    | User B    | User C  |
| 3    | User A    | User C  |
| 4    | User D    | User B  |
| 5    | User B    | User A  |
| 6    | UserE     | User B  |
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