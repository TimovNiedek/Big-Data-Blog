---
layout: page
title: Big Data Blog
description: Assignment 2
---

In this blog post I will reflect on my experiences when analysing the open data sets of the city of Nijmegen.

### Loading the BAG data

The BAG data set contains basic records on adresses and buildings. We can extract the street names, quarter and x and y locations from these records. We call these reduced records "addresses".

We can then take the quarters with the most inhabitants using:

`addresses.map(a => (a(1), 1)).reduceByKey(_ + _).sortBy(-_._2).collect()`

The `sortBy` fuunction takes an argument that defines the sorting order. We use descending (`-`) and the second parameter (`_._2`) which corresponds to the counts.

### Data Frame API

Using the Data Frame API we create a class `Addr`, which can be used to represent the data more easily. However, some x and y coordinates are poorly formatted: they are empty. There are 45 records where this is the case. There are a couple of options of dealing with this problem:

* Remove the records entirely. This is a good solution if we don't want the invalid data when we are analysing properties of the data set that are dependent on the x and y values. We still get a good coverage, because there are only 35 out of 100k records invalid.
* Set the x and y values to 0. This is a better solution if we want to analyse properties using the street and quarter, such as counting. If we want precise counts, we have to include those records. That is, if the domain expert says that there are still people living on that address.

We use the first solution, because this leads to a pure data set, where no records have been artificially modified. Setting x and y to 0 still has meaning, and may lead to problematic results when computing means or other statistical varaiables.

We can sort the quarters by descending number of addresses using:

`val qc_1 = addrDF.groupBy("quarter").count.sort(desc("count"))`

Getting the top 10 is then as easy as calling:

`val qc_1_top = qc_1.limit(10)`

### Using SQL

We can use DataBricks as an alternative CSV parsing library. This library can be used to create a DataFrame from a CSV file directly. We use this library to read the `vBoaEvents_all.csv` file, a file containing information about events in Nijmegen. We also load `vBoaEventsLoc_all.csv`, containing information about the location of these events.

The relevant information from `vBoaEvents_all.csv` is:

* eventid: integer
* typeid: integer
* typenaam: string
* statusid: integer
* statusnaam: string

The relevant information from `vBoaEventsLoc_all.csv` is:

* eventid: integer
* straat: string
* x_koord: string, manually converted to float
* y_koord: string, manually converted to float

These two tables are joined by the eventid into a table called `el`.

### Stadsgetallen

The next dataset we will load is the `opendata_stadsgetallen.accdb` set. This set is first converted to csv format using the access2csv package. The set contains many statistics about the city's inhabitants. We join this table by quarter on the `el` table to analyse them. The resulting table has the columns

| Waarde | WaardetypeNaam | ThemaNaam | OnderwerpNaam | Labelgroepnaam | LabelNaam | Wijk | TijdOmschrijving | EventId |
|:------:|:--------------:|:---------:|:-------------:|:--------------:|:---------:|:----:|:----------------:|:-------:|

Now, we can perform analysis on this table. We want to analyse the age of people living near certain events. We do this by only selecting the rows where the `OnderwerpNaam = Geslacht en leeftijd`. Then, if we sort this table descending by `TijdOmschrijving`, we can see that the most recent data is from `1-1-2012`. We only select the data from this last year, in order to avoid duplicate data from older measurements. We are left with 4845 rows of data.

We can count the number of events per quarter by running `SELECT quarter, COUNT(event) FROM eventsIn2012 GROUP BY quarter`.
This shows that the most events in 2012 happened in the city center (logically), with 1330 events.



