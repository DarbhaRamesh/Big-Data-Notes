
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
