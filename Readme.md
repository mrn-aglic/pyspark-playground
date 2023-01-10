# About the repo

I started this repo because I wanted to learn PySpark.
However, I also didn't want to use Jupyter notebook as it
is typically the case in the examples I came across. 

Therefore, I started with setting up a spark cluster 
using docker. 

# Running the code (Spark standalone cluster)
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

## Web UIs
TODO


# Running the code (Spark on Hadoop Yarn cluster)
Before running, check the virtual disk size that Docker
assigns to the container. In my case, I needed to assign
some 70 GB.
You can run Spark on the Hadoop Yarn cluster by running:
```shell
make run-yarn
```
or with 3 data nodes:
```shell
make run-yarn-scaled
```
You can submit an example job to test the setup:
```shell
make submit-yarn-test
```
which will submit the `pi.py` example in cluster mode.

You can also submit a custom job:
```shell
make submit-yarn-cluster app=data_analysis_book/chapter03/word_non_null.py
```

There are a number of commands to build the cluster,
you should check the Makefile to see them all. But the
simplest one is:
```shell
make build-yarn
```

## Web UIs
You can access different web UIs. The one I found the most 
useful is the NameNode UI:
```shell
http://localhost:9870
```

Other UIs:
- ResourceManger - `localhost:8088`
- Spark history server - `localhost:18080`

# Stories published on Medium
1. Setting up a standalone Spark cluster can be found [here](https://medium.com/@MarinAgli1/setting-up-a-spark-standalone-cluster-on-docker-in-layman-terms-8cbdc9fdd14b).
2. 

# About the book_data directory
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