# simpleSqlite
This is a B+ tree based sql implemendation from scratch which supports basic sql commands insert and select and will update the modifications into a specific database file after the execution of exit or quit command.

## B+ Tree:

B tree is a self-balancing tree data structure that maintains sorted data and allows searches, sequential access, insertions, and deletions in logarithmic time. The B-tree generalizes the binary search tree, allowing for nodes with more than two children.
![1920px-B-tree svg](https://user-images.githubusercontent.com/83719401/184690234-9d6dd22f-cf4f-46e7-8584-eb7be5db1914.png)

Unlike normal B-tree which has key-value pairs contained in every single nodes, B+ tree has key-value pairs only contained in leaf node which largely shortened the times of IO operations.
![ds_13](https://user-images.githubusercontent.com/83719401/184690844-a11a0d3a-de53-4a53-8116-12285bfd7cbb.jpg)

## commands:

### meta commands(with a leading . ):
1 .exit / .quit : quit and save the modifications.

2 .constant : show constants info

3 .btree : show btree info

4 .clear : clean the screen

### normal commands(without a leading .):
1 insert [id] [name] [mail]

2 select

## how to use:

in linux environment:

make db
