ne more gist related to controlling the number of mappers in a mapreduce task.
 
Background on Inputsplits
--------------------------
An inputsplit is a chunk of the input data allocated to a map task for processing. FileInputFormat
generates inputsplits (and divides the same into records) - one inputsplit for each file, unless the
file spans more than a HDFS block at which point it factors in the configured values of minimum split
size, maximimum split size and block size in determining the split size.
 
Here's the formula, from Hadoop the definitive guide-
Split size = max( minimumSplitSize, min( maximumSplitSize, HDFSBlockSize))
So, if we go with the default values, the split size = HDFSBlockSize for files spanning more than an
HDFS block.
 
Problem with mapreduce processing of small files
-------------------------------------------------
We all know that Hadoop works best with large files; But the reality is that we still have to deal
with small files. When you want to process many small files in a mapreduce job, by default, each file
is processed by a map task (So, 1000 small files = 1000 map tasks). Having too many tasks that
finish in a matter of seconds is inefficient.
 
Increasing the minimum split size, to reduce the number of map tasks, to handle such a situation, is
not the right solution as it will be at the potential cost of locality.
 
Solution
---------
CombineFileInputFormat packs many files into a split, providing more data for a map task to process.
It factors in node and rack locality so performance is not compromised.
 
Sample program
---------------
The sample program demonstrates that using CombineFileInput, we can process multiple small files (each file
with size less than HDFS block size), in a single map task.
 
Old API
--------
The new API in the version of Hadoop I am running does not include CombineFileInput.
Will write another gist with the program using new API, shortly.
 
Key aspects of the program
----------------------------
1. CombineFileInputFormat is an abstract class; We have to create a subclass that extends it, and
implement the getRecordReader method. This implementation is in the class -ExtendedCombineFileInputFormat.java
(courtesy - http://stackoverflow.com/questions/14270317/implementation-for-combinefileinputformat-hadoop-0-20-205)
2. In the driver, set the value of mapred.max.split.size
3. In the driver, set the input format to the subclass of CombineFileInputFormat 