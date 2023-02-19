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
The master node can be accessed on:
`localhost:9090`. 
The spark history server is accessible through:
`localhost:18080`.


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

# About the branch expose-docker-hostnames-to-host
The branch expose-docker-hostnames-to-host contains the 
shell scripts, templates, and Makefile modifications 
required to expose worker node web interfaces. To run 
the cluster you need to do the following. 
1. run
```shell
make run-ag n=3
```
which will generate a docker compose file with 3 worker
nodes and the appropriate yarn and hdfs site files
in the yarn-generated folder.
2. register the docker hostnames with /etc/hosts
```shell
sudo make dns-modify o=true
```
which will create a backup folder with your original
hosts file.

Once you are done and terminate the cluster, restore 
your original hosts file with:
```shell
sudo make dns-restore
```

For more information read the story I published on Medium
[here](https://medium.com/@MarinAgli1/using-hostnames-to-access-hadoop-resources-running-on-docker-5860cd7aeec1).


# Stories published on Medium
1. Setting up a standalone Spark cluster can be found [here](https://medium.com/@MarinAgli1/setting-up-a-spark-standalone-cluster-on-docker-in-layman-terms-8cbdc9fdd14b).
2. Setting up Hadoop Yarn to run Spark applications can be found [here](https://medium.com/@MarinAgli1/setting-up-hadoop-yarn-to-run-spark-applications-6ea1158287af).
3. Using hostnames to access Hadoop resources can be found [here](https://medium.com/@MarinAgli1/using-hostnames-to-access-hadoop-resources-running-on-docker-5860cd7aeec1).

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
