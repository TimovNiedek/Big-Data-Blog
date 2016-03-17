---
layout: page
title: Big Data Blog
description: Assignment 2
---

In this blog post I will reflect on my experiences when trying to get a Spark Notebook running in docker.

### Setup

The main part of this assignment was done on a computer in terminal room HG00.023, running Ubuntu. This blog was written using my own laptop, simply because I wanted to have a local version of this blog for archiving purposes. 

To get a feeling for the structure of the docker image, I have started a shell in the image, using

`docker exec -it HASH /bin/bash`

and then showing the structure using `ls` and `cd`.

I copied the *Complete Shakespeare* text to the `/data` directory manually. The instructions said that in the terminal rooms we had to use the command:

`scp USERNAME@hg023pcXX:/vol/practica/BigData/100.txt.utf-8 /data`

However, I used `lilo.science.ru.nl`, instead of the machine name, which seemed to work just fine. Copying the Spark Notebook to the image was straightforward, the commands given in the instructions worked immediately for me.

## Big Data Spark 101

The *Big Data Spark 101* notebook provides some sample scala code blocks for demonstrating the usage of Spark. I have used the [Spark Programming Guide](https://spark.apache.org/docs/latest/programming-guide.html) for additional information about what each command does to the RDD. I tried to get the python notebook "*prabeeshk/pyspark-notebook*" working in the docker image, with no success. I wanted to see what this notebook looked like because I have experience using Python, and none with using Scala. However, using the Spark Programming Guide, I was still able to figure out what each command does.

#### Parallelize

The first command creates a simple RDD containing the numbers from 0 to 999. This RDD is created using `parallelize`, with 8 partitions. Then, a command called `takeSample` is called on the RDD, creating two jobs (as can be viewed ath the *Spark Jobs* screen). One of these jobs is for counting the members of every partition, the other one for taking a random amount of members from it. Every job has 8 tasks, one for each partition. 

#### Creating an RDD from Shakespeare

The next two lines can be used to check if copying Shakespeare's work went okay, the last of these lines should return something containing `100.txt.utf08`.

Then, a new RDD is created from this text file. Note that this is not immediately visible, but the value `lines` is a RDD with type `[String]`, one element contains one line of the file. This RDD has two partitions on my machine. This value was found as follows: 

	val lines = sc.textFile("/data/100.txt.utf-8")
	println(lines.partitions.length)
	// Returns 2

Spark defaults to creating one partition for each HDFS block. The HDFS partition size is 64 MB, while the Shakespeare text file is 5.6 MB, well under one block size. So why does Spark create two partitions, instead of one?

The default mininum amount of partitions is equal to a value found in `spark.defaults.parallelism`. This value is equal to the number of cores across all the machines in the cluster, according to [this StackOverflow answer](http://stackoverflow.com/questions/26368362/how-does-partitioning-work-in-spark). Apparently, this value is two, so there are two cores in our simulated cluster.

#### Actions on our RDD

First of all, we can count the number of lines and characters that the input text has. The `count` action simply returns the number of elements in the dataset, which is split by every line. We see that there are 124787 lines. Then, for counting the number of characters, we need a map-reduce structure. The mapper transforms every line to the number of characters in that line (or the `length`) of that line. The reducer then adds up all the line's lengths.

These actions result in two stages: the `count` stage and the `reduce` stage. The `map` is contained within the `reduce` stage, but it is not a seperate job. The reason for this is that map can be lazily evaluated. All of these stages and jobs contain 2 tasks, one for each partition. In my simulation, the first partition has 62608 records (or lines), and the second partition has 62179 records.

For more interesting operations, we need to modify the dataset to contain not lines but seperate words. This is done by mapping through all the lines, and splitting them at spaces. The empty words are removed with a `filter` operation. These emtpy words can be the result of a space at the end of a line, or a line being completely empty. The result of this filter is then mapped from a collection of strings to a collection of tuples `(word, 1)`. This is neccessary to count the occurrences of words, as will be evident in the next line. Note that the result of the flatMap-filter-map actions is a new RDD with type `(String, Int)`.

The next action reduces this RDD to a new RDD containing the counts of each word:

	val wc = words.reduceByKey(_ + _)

Again, `wc` is a new RDD with type `(String, Int)`. If we first take 10 elements from this array, and then take the top 10 words sorted by occurrence we can see in the Jobs panel that one stage is skipped. The skipped stage is the flatMap-filter-map stage. However, this stage is not skipped when we take 10 elements the first time. This suggests that the results of this stage are stored, and later retrieved when we take another 10 elements. Note that `take` and `takeOrdered` do not generate a new RDD, unlike map or reduce operations. Therefore, the `map` functions in the next line are not RDD actions but simply array operations.

The `cache()` action stores the RDD in memory, in order to make future operations faster. However, this was not noticable in my simulation. Note that the storage of the word count RDD is not immediately visible in the Storage tab of the SparkUI. Instead, the RDD is stored only when another action is applied to the RDD (in this case a `collect`). Then, the RDD is stored in memory, which is visible in the Jobs tab (as `saveAsTextFile`) and after that the `collect` action is exectuted. Now, the word count RDD is visible in storage. 

The line after collecting the count of "Macbeth" saves the words RDD to disk, instead of only memory, which results in two files, one for each partition.

#### Case insensitivity

In the previous steps, all the words were saved with punctuation marks, and capitalized words and non-capitalized words are seperate entries. To remove punctuation and make all words lowercase, we have to add another map in the tokenization step. If we compare the count of the word "macbeth" with the count of the word "Macbeth" in the RDD without the new tokenization, we can see that removing punctuation and transforming to lowercase results in a way higher count for that word. Without this transformation, the count for "Macbeth" was 30, now we get 284 occurrences.

It is very difficult, but certainly possible to adapt the tokenization step to count bigrams instead of unigrams. This is difficult because the line is split for whitespaces. After this split, it is difficult to merge two words into one bigram by mapping.

## Big Data: Execution Model

The next part will refer to the Spark notebook *big-data-execution-model*.

In this notebook an RDD is created with numbers from 0 to 999. It has 8 partitions, but no partitioner. This RDD is then mapped to create an RDD with some number pairs. Note that there are still no jobs or stages in the SparkUI, because `map` is a transformation, and transformations are lazily evaluated. Only when the next `take` action is executed, a job and stage for that job will appear in the UI.

#### Adding a partitioner

The default RDD does not have a partitioner. Therefore, it defaults to using 8 partitions when creating new RDDs from that RDD. However, when we add a HashPartitioner with two partitions, the original RDD is still 8 partitions, but after creating a new RDD with the transformation `groupByKey`, this RDD has only two partitions.

But does this new RDD also have a partitioner? It appears that this depends on which operations are executed on the RDD. Certain operations guarantee that the partition sizes remain the same. If this is the case, the partitioner will be carried over to the new RDD. Otherwise, the partitioner is lost and we have no control over the amount of partitions.

We can regain this control by using the `repartition` operation. This operation adds another RDD with 2 partitions above the original RDD (which, in the example had 4 partitions). We can also `coalesce` the original partitions, which replaces these with partitions of a new size.

The difference between these two methods becomes visible when we take a sample from each variant. If we take a sample from the `repartition`ed RDD, the following steps are executed:

- Repartition the RDD with 4 partitions to 2 (4 tasks)
- Take a sample from the repartitioned RDD (2 tasks)

If we take a sample from the `coalesce`d RDD, we only get one task: the task for taking a sample. Also, this query plan has only one shuffle read of 12.0 KB, while the repartitioned query plan has two shuffle reads (in total 19.9 KB) and one shuffle write of 7.9 KB.

The reason for this is that when `repartition` is called, the data is reshuffled randomly to create new partitions. All data is read (12.0 KB), shuffled and written (7.9 KB) and read again when taking a sample (7.9 KB). When `coalesce` is called, the partitions are merged and only one read when taking a sample is neccessary (12.0 KB).

Note that the results differ between these two query plans, because the `repartition` query stems from rddA, which was created using `rddp4.values.map(x => x + 10)`, which returns an RDD of type [Int]. The `coalesce` query stems from rddB, which was created using `mapValues`, which in turn returns an RDD of type [(Int, Int)].

We can conclude from this that `repartition` is best used when you want to balance data over a certain number of partitions, while `coalesce` is best used when you want to merge certain partitions, because it is more efficient.