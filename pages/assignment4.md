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

The commoncrawl data had already been prepared on the [SurfSara Hadoop cluster](https://userinfo.surfsara.nl/systems/hadoop/description). To authenticate to this cluster, [Kerberos](http://web.mit.edu/kerberos/) is used. 

### Reading the Commoncrawl

The many websites of the commoncrawl are stored as Web Archive files - or *WARC* files. A textual version of a WARC file can be found [here](https://gist.github.com/Smerity/e750f0ef0ab9aa366558#file-bbc-warc). Instead of importing the WARC files as text, a more accessible interface is provided in the form of the WarcRecord class. I found it very helpful to use the [grepcode entry for the WarcRecord](http://grepcode.com/file/repo1.maven.org/maven2/org.jwat/jwat-warc/0.9.0/org/jwat/warc/WarcRecord.java) for finding helpful properties. Especially the `getPayload` and `getHttpHeader` properties are useful for extracting the data from the WarcRecords.

### First Steps

To create a program for analysing the commoncrawl data from scratch, it is very unpractical to be constantly pushing to the SurfSara cluster. Instead, I started development inside the `andypetrella/spark-notebook` container. This container is used to develop Spark programs using notebooks. Notebooks are a way of creating code dynamically, by allowing you to write your code in blocks that can be run independently. This makes it incredibly easy to debug your code and experiment with new snippets. The Spark Notebook is not supported on the SurfSara cluster, which is why we used it locally to create and test our software.

