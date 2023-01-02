# About the repo

I started this repo because I wanted to learn PySpark.
However, I also didn't want to use Jupyter notebook as it
is typically the case in the examples I came across. 

Therefore, I started with setting up a spark cluster 
using docker. 

## Running the code
You can run the spark standalone cluster by running:
```shell
make run
```
or with 3 workers using:
```shell
make run-scaled
```
You can submit Python jobs with the command:
```shell
make submit app=dir/relative/to/spark_apps/dir
```
e.g. 
```shell
make submit app=data_analysis_book/chapter03/word_non_null.py
```

There are a number of commands to build the standalone cluster,
you should check the Makefile to see them all. But the
simplest one is:
```shell
make build
```

## About the book_data directory
The official repo of the book Data Analysis with Python and
PySpark can be found here:https://github.com/jonesberg/DataAnalysisWithPythonAndPySpark.

I did not include the files from this repo as there are
files larger than 50 MB which is the limit for GitHub. At the
time of writing the repo contains a link to Dropbox which
contains the files. I suggest you download them.

The book_data directory contains some files found on
this repo:
https://github.com/maprihoda/data-analysis-with-python-and-pyspark.
Which is also a repo of someone who read the mentioned book.