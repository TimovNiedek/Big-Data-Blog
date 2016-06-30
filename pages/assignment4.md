---
layout: page
title: Big Data Blog
description: Assignment 4: Finding Profanities in the Commoncrawl Data
---

The [commoncrawl](http://commoncrawl.org/) is a collection of very, very many websites. The billions of pages can be analysed by anyone to find interesting statistics about all kinds of different websites that have been stored. In this blog post, I will explain how we accessed this data and performed analysis on it. I have created a simple program that can count the number of profanities in a website. I will explain how I created this program, and highlight some important challenges when working on a cluster for large-scale text mining.

### Setup

First of all, I will explain my setup. I have installed docker on my own computer, inside an Ubuntu 14.04 LTS virtual machine. Installing docker on Linux is quite straightforward: these steps can be found on Docker's [website](https://docs.docker.com/engine/installation/linux/). For further setup, I mostly followed the structure on the courses [assignment description](http://rubigdata.github.io/course/assignments/A4-commoncrawl.html), adapting them to my needs whenever possible.

I used two docker images for building, testing and deploying our software. The first is a container for creating scala notebooks to create Spark software, and can be installed with the following command:

	docker pull andypetrella/spark-notebook:0.6.2-scala-2.11.7-spark-1.6.0-hadoop-2.7.1-with-hive-with-parquet


The second image is a Hathi client for deploying software to the cluster and running it. It can be pulled with the command `docker pull surfsara/hathi-client`.

### Connecting to the SurfSara cluster

The commoncrawl data had already been prepared on the [SurfSara Hadoop cluster](https://userinfo.surfsara.nl/systems/hadoop/description). To authenticate to this cluster, [Kerberos](http://web.mit.edu/kerberos/) is used. I did not encounter many problems with authenticating to the SurfSara cluster when following the assignment's instructions.

### Reading the Commoncrawl

The many websites of the commoncrawl are stored as Web Archive files - or *WARC* files. A textual version of a WARC file can be found [here](https://gist.github.com/Smerity/e750f0ef0ab9aa366558#file-bbc-warc). Instead of importing the WARC files as text, a more accessible interface is provided in the form of the WarcRecord class. I found it very useful to use the [grepcode entry for the WarcRecord](http://grepcode.com/file/repo1.maven.org/maven2/org.jwat/jwat-warc/0.9.0/org/jwat/warc/WarcRecord.java) for finding helpful properties. Especially the `getPayload` and `getHttpHeader` properties are useful for extracting the data from the WarcRecords.

### First Steps

To create a program for analysing the commoncrawl data from scratch, it is very unpractical to be constantly pushing to the SurfSara cluster. Instead, I started development inside the `andypetrella/spark-notebook` container. This container is used to develop Spark programs using notebooks. Notebooks are a way of creating code dynamically, by allowing you to write your code in blocks that can be run independently. This makes it incredibly easy to debug your code and experiment with new snippets. The Spark Notebook is not supported on the SurfSara cluster, which is why we used it locally to create and test our software.

I used the provided [WARC for Spark notebook](http://nbviewer.jupyter.org/url/rubigdata.github.io/course/assignments/BigData-WARC-for-Spark.snb) as a starting point for developing my program. This notebook shows how to load WARC files as RDDs in Spark. To use this notebook, I needed to download the dependencies manually:

	wget http://beehub.nl/surfsara-repo/releases/nl/surfsara/warcutils/1.3/warcutils-1.3.jar
	wget http://central.maven.org/maven2/org/jwat/jwat-common/1.0.0/jwat-common-1.0.0.jar
	wget http://central.maven.org/maven2/org/jwat/jwat-warc/1.0.0/jwat-warc-1.0.0.jar
	wget https://jsoup.org/packages/jsoup-1.9.2.jar

The warcutils package is a useful package for reading the WARC files: it parses them into a WarcInputFormat which can be used to create an RDD. The jwat packages contain classes that describe the WARC format such as the `WarcRecord` described before and the `WarcHeader` class containing information about the record. The jsoup package is used to transform the raw HTML contents to text.

The next step is to create an RDD corresponding to one WARC file. To do this, we add the following lines:

	val warcf = sc.newAPIHadoopFile(
              warcfile,
              classOf[WarcInputFormat],               // InputFormat
              classOf[LongWritable],                  // Key
              classOf[WarcRecord]                     // Value
    ).map{wr => wr}.cache()

Just like stated in the provided [notebook](http://nbviewer.jupyter.org/url/rubigdata.github.io/course/assignments/BigData-WARC-for-Spark.snb), I had to add the extra (non-functional) map before caching the RDD. If we don't do this, we get a `java.io.IOException: Stream closed` later in the code (for unknown reasons).

### Getting the information

The resulting RDD has `WarcRecord` values. First of all, only the records where the WARC-type was a response are kept. See [this page](http://archive-access.sourceforge.net/warc/warc_file_format-0.16.html#field_warc-type) for the possible WARC-types. These records are further filtered: only the WARCs with contentType `text/html` are kept. I discovered that some records did not have a contentType (or one that was set to `null`). This led to some nasty NullPointerExceptions, which are always fun to debug... (more on debugging below)

To remedy this, I modified the code in the provided notebook as follows:

	val warcHtml = warcf.
	    filter{ _._2.header.warcTypeIdx == 2 /* response */ }.
	    filter{ _._2.getHttpHeader().contentType != null}.
		filter{_._2.getHttpHeader().contentType.startsWith("text/html") }

The second filter removes the records that don't have a contentType.

The resulting RDD still contains `WarcRecord` values. The relevant information for my task is the domain of the website and the actual contents of the website. To get the domain, I first retrieve the `warcTargetUriStr` and convert it to the domain name with the following helper function:

	def getDomain(uri: String):String = {
	  val host = {new URI(uri)}.getHost()
 	  if (host.startsWith("www.")) host.substring(4) else host
	}

Notice that on the second line inside the function, I also remove the `www.` before domain names. This is neccessary to properly group the domains later; we don't want different records for `www.rubigdata.github.io` and `rubigdata.github.io`.

To get the contents of a website, I used the provided helper functions `getContent` and `HTML2Txt`. Combining the content and domain retrieval in one single map, we get the following map function:

	val warcc = warcHtml.map(wr => (getDomain(wr._2.header.warcTargetUriStr),
    	                            HTML2Txt(getContent(wr._2))))

This function creates a PairRDD for us containing the domain name as key and the contents as values.

### Let's Tokenize!

To count the number of profanities in a text, we first need to turn the contents of the websites into words. To do this, we remove all punctuation, make the contents lowercase, split by whitespace and remove the empty words. Finally, I found out in a later stage of development that some websites contained no words. We remove these records now instead of later for performance reasons: we don't need to check them any more. This results in the following snippet of code:

	val contents = warcc.map(wr => (wr._1, wr._2
    		.replaceAll("""[\p{Punct}]""", "")
    	    .toLowerCase
    	    .split(" ")
    	    .filter(_ != "")))
    	.filter({_._2.length > 0})

### Finding profanities

Now comes the fun part: we get to see which domains are naughty and contain profanities. The first step is to find a nice big list with all kinds of horrible words that a mother would never want their children to see. I found such a list on the website of [FrontGate media](http://www.frontgatemedia.com/a-list-of-723-bad-words-to-blacklist-and-how-to-use-facebooks-moderation-tool/), which seems to be a website for learning to advertise to a Christian audience ([this](http://www.frontgatemedia.com/new/wp-content/uploads/2014/03/Terms-to-Block.csv) is the actual list, viewer discretion is advised). I removed the header of this comma-separated file manually.

Remember that we were still working locally, in the Spark Notebook container. I copied the list of profanities into the container and loaded it using the following code:

	val profRDD = sc.textFile("/data/profanities.csv", 2)
		.map(p => p.replaceAll("""[\p{Punct}]""", ""))

Again, we remove all punctuation from the profanities. We actually don't need to save the profanities as an RDD since it is pretty small (only 723 entries). Therefore, we convert it to a set:

	val profanities = profRDD.collect().toSet

Well, let's do the sensible thing and see if there is any swearing in the course page!

	val profanitiesPerWarc = contents.map(wr => (wr._1,
										  wr._2.filter(filtered.contains(_)).length))
	val profanitiesPerDomain = profanitiesPerWarc.reduceByKey(_+_)

	// Don't do this when working on the cluster!
	// I added this under the assumption that only the WARC for 
	// rubigdata.github.io is loaded.
	profanitiesPerDomain.collect()

Fortunately, there isn't. We can safely advertise the Big Data course to interested Christians.

### Finding the profanity-density

Of course, some sites have more content than others, and therefore might score artificially high on a simple profanity count. We might want to allow some small slip-ups for big sites, but not more. To get a better metric for level of profanity, I introduced the concept of profanity-density. The profanity-density is defined as the number of profanities in a document divided by the number of words in that document.

To find this, we first need to do a simple word count:

	val wordCount = contents.map(wr => (wr._1, wr._2.length))
	val wordsPerDomain = wordCount.reduceByKey(_+_)

The profanity-density is then found by adding the following lines:

	val profanityDensity = profanitiesPerDomain
		.join(wordsPerDomain)
        .map{case(k, (ls, rs)) => (k, ls.toFloat / rs)}

### Scaling up & Debugging

Now that we've tested our Spark app on some test data, we can try it out on the cluster. To do this, we move our app from the Spark Notebook container to the surfsara/hathi-client. In this container, I created the self-contained RUBigDataApp following the [provided instructions](http://rubigdata.github.io/course/assignments/sbt.html). I was able to run this app with no problems. For convenience, I named my profanity counter `RUBigDataApp.scala` and replaced the given scala file at `/hathi-client/spark/rubigdata/src/main/scala/org/rubigdata/` with my own.

Building it using sbt led to some problems: the notebook was not immediately useable as a standalone Spark app. I moved all the imports to the top of the file, and wrapped my code in a `main` function. The `:cp` trick to add dependencies was not needed anymore, since this is handled by sbt. The code for registering classes to the Kryo serializer did not work: the variable `conf` was not found. The solution was to add them using the following code instead:

	val conf = new SparkConf().setAppName("RUBigDataApp")
		.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		.set("spark.kryo.classesToRegister",
			"org.apache.hadoop.io.LongWritable," +
			"org.jwat.warc.WarcRecord," +
			"org.jwat.warc.WarcHeader," +
			"org.apache.spark.rdd.RDD")

I tried to run this app on the SurfSara cluster. However, this did not immediately work: I got a `ClassNotFoundException` for a class in the warcutils jar. After verifying that they were in fact included in the sbt build process, I tried creating the jar with `sbt assembly`. After pushing the resulting jar and running it, the classes were found. I ran my app with the WARC file at:

	/data/public/common-crawl/crawl-data/CC-MAIN-2016-07/segments/1454701165302.57/warc/CC-MAIN-20160205193925-00246-ip-10-236-182-209.ec2.internal.warc.gz`

However, this time I got the `NullPointerException` (which was described above in the section **Getting the information**). I found it incredibly difficult to debug this by logging the variables on the cluster. However, the exception only appeared when I ran my app using above WARC file. I had no clue what this WARC file looked like, so I downloaded it with the command:

	hdfs dfs -get /data/public/common-crawl/crawl-data/CC-MAIN-2016-07/segments/1454701165302.57/warc/CC-MAIN-20160205193925-00246-ip-10-236-182-209.ec2.internal.warc.gz /path/to/target

I then copied the WARC file to the host from the `surfsara/hathi-client` container and copied it into the `andypetrella/spark-notebook` container (at this moment, docker does not support copying between containers). Then, I used this notebook to find the cause for the `NullPointerException`. The solution was, as described before, to remove entries that had no contentType.