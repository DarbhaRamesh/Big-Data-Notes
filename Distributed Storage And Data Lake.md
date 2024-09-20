
In general, Hadoop cluster is not accessible through name node instead a gateway node is present on top of the cluster. Clients will login to gateway node to access the Hadoop cluster 

##### Linux commands

Present working directory - Gives you the directory we are in

``` console
pwd 
```

To get the user who logged in

``` console
whoami
```

Linux follows a tree like structure

/ (root) - top most directory

cd  - change directory

``` console
cd /
```

To navigate to your home directory 

``` console
cd ~
```

one directory back

``` console
cd ..
```

Go back to directory where your previous in

``` console
cd -
```

long list the files/directory in a directory (permissions, links, owner, group, size, created at, name). based on alphabetical order of file/directory name in ascending order

``` console
ls -l
```

absolute path - complete path from root
relative path - path from the directory your in

long list files based on created at in desc order

``` console
ls -lt
```

in ascending order (r - reverse order)

``` console
ls -ltr
```

Recursive listing

``` console
ls -lR
ls -lR /dir1/dir2/
```

to list the hidden files (a - all files/directories including hidden files/directories)
hidden files start with dot (.)

``` console
ls -a
```

create an empty file or to change the create timestamp to current one

``` console
touch file1.txt
```

permissions on file/ directory will be in below order

owner group others

rwx rw- r--

r- read (4)
w - write (2)
x- execute (1)

664
owner - read & write
group  - read & write
others - read

change permissions on a file/ directory

``` console
chmod 777 file1.txt
```

To read content in a file

```console
cat file1.txt
```

create new directory

``` console
mkdir dir1
```

remove/delete a directory - when directory is empty

```console
rmdir dir1
```

remove/delete a file

``` console
rm file1.txt
```

If directory is not empty and have to delete a directory

``` console
rm -R dir1
```

cp command - used to copy files and directories

```console
touch file1
cp file1 file2
```

copy a file into an existing directory

``` console
cp file1 dir1
```

copy directory to another directory

``` console
cp -R dir2 dir3
```

mv command - move a file from one directory to another directory (cut - paste)
can also be used to rename a file

``` console
touch file56
mv file56 file5
mv file5 dir3
```

create a file with content (w - save, q - quit, ! - forcefully)

``` console
vi samplefile
i
enter some data
esc
:wq!
```

To print first n lines in a file (default 10)

``` console
head file1
head -5 file1
```

last n lines

``` console
tail file1
taile -5 file1
```

history of commands

``` console
history
```

details of a file (lines, words, characters)

``` console
wc file1.txt
```

create a file with content

``` console
cat > file3.txt
this is a sample file
ctrl + d
```

append content

``` cosnole
cat > file1.txt
this is file1
ctrl + d

cat file1.txt file3.txt >> mergedfile.txt
```

disk usage (-h human readable size) - size of directory/file

``` console
du -h /dir1/
```

search a word in a directory/ file

``` console
grep file *
```

---

##### HDFS commands

List of all the Hadoop commands available

``` console
hadoop fs
hdfs dfs
```

List files/directories in HDFS in your home directory

in local, your home directory will be in /home/
in HDFS, your home directory will be in /user/

``` console
hadoop fs -ls /user/youruserid
hadoop fs -ls
```

Most of the hdfs commands will interact with name node. only when we are reviving data then it will interact with data node

list files in HDFS based on file size in desc order

``` console
hadoop fs -ls -S -h /
```

filter results from ls command

``` console
hadoop fs -ls /user | grep user1
```

create directory inside another non existing directory (-p pattern)

``` console
hadoop fs -mkdir -p /user/user1/dir1/dir2
```

upload a file from local to HDFS

``` console
hadoop fs -put <local file path> <hdfs path>
hadoop fs -copyFromLocal <local file path> <hdfs path>
```

download a file from HDFS to local

``` console
hadoop fs -get <hdfs path> <local path>
hadoop fs -copyToLocal <hdfs path> <local path>
```

cp command in Hadoop will be slow as the data is copied to a new file. instead mv will be quick as only metadata will be changed

some useful commands to get info about the HDFS and details of file in HDFS

``` console
hadoop fs -df -h /user/userid
hadoop fsck <hdfs file path> -files -blocks -locations
```

---

##### HDFS vs Cloud Data Lakes

HDFS is distributed file system and internally data is stored in blocks and is not persistent.
- After you terminate the HDFS cluster, you loose the data stored in HDFS.
- Incurs more cost as you have to keep you cluster always on, even to store the data.
- works is silos. data in one cluster cannot be accessed by other cluster.

Cloud Data Lakes is Object based storage and is persistent. Its a cost effective solution.
- Object contains 
	- id - unique identifier
	- value - actual content
	- metadata 
---

##### MapReduce

MapReduce intuition is very important which forms basics for Spark.

Its a **programming paradigm** which has two phases - **Map & Reduce**. where both takes input and produces output in **key, value pair**.

code written by user will be packages and sent to each data node. First in all the data nodes, the **mappers** would run in parallel  on the block available in the node. The data is processed on the same node, this principle is called **Data Locality**.

How many mappers will run? which is equivalent to number of blocks. But parallelism is based on number of data nodes. 

Once all the map activity is completed in all the data nodes then output of the mapper is sent to one among these data nodes where **reduce** activity is performed. After this we get the desired output. 

The data will be residing in a file. How will this data be converted to key value pair for mapper input?
The data from the block is converted to key value pair by **record reader** where key is the address of the each line and value is the each line. This will be the input to mapper.

Bringing the mapper output to one node is called **shuffling activity**. After this, the data is sorted based on the key which is called **sorting activity**. In short, before a reduce activity, **shuffle-sort** activity happens. This activity is performed by Hadoop framework. As a programmer, we only write map code and reduce code.

Since parallelism is achieved by mapper, **more processing should be at mapper end**. If more processing is at reducer end then we are deviating from the distributed framework as most of the processing is done in single node.

---
