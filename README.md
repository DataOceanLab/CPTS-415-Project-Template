# CPTS-415-Project-Template

This template contains a demo program for Spark SQL and GraphFrame, written in Scala and SQL

## Requirement

The following script works on an Ubuntu Linux machine and Spark 3.0.1 and Scala 2.12.8

## Set up your environment

In your linux terminal, run the following:

### Install Java JDK (only if you don't have JDK on your machine)

https://docs.datastax.com/en/jdk-install/doc/jdk-install/installOpenJdkDeb.html

### Install SBT (Scala Building Tool)

See https://www.scala-sbt.org/release/docs/Installing-sbt-on-Linux.html#Ubuntu+and+other+Debian-based+distributions

```
echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo apt-key add
sudo apt-get update
sudo apt-get install sbt
```

### Download Spark

```
curl https://archive.apache.org/dist/spark/spark-3.0.1/spark-3.0.1-bin-hadoop2.7.tgz -o spark-3.0.1-bin-hadoop2.7.tgz
tar -xzf spark-3.0.1-bin-hadoop2.7.tgz
```

### Start Spark cluster (1 master node and 1 worker node)

```
./spark-3.0.1-bin-hadoop2.7/sbin/start-all.sh
```

### Check your Spark cluster

1. Open your web browser, go to `http://localhost:8080/`
2. You should have seen a Spark web interface with 1 worker node
3. Record the master IP address - Spark Master at XXX
	- On my machine, it is `spark://Jias-MacBook-Pro.local:7077`


## How to submit your code to Spark cluster
1. Go to the root folder of this repo
2. Run ```sbt assembly``` in the terminal. You need to have "sbt" installed in order to run this command.
3. Find the packaged jar at this path: `target/scala-2.12/CPTS-415-Project-assembly-0.1.0.jar`
4. Submit the jar to Spark using Spark command
	- On my machine, it is `./spark-3.0.1-bin-hadoop2.7/bin/spark-submit --master spark://Jias-MacBook-Pro.local:7077 ~/GitHub/CPTS-415-Project-Template/target/scala-2.12/CPTS-415-Project-assembly-0.1.0.jar`
	- You should replace the master IP and the absolute path of the jar with your own info on your machine
5. You can open your browser and go to `http://localhost:8080/`. You should see the application has been submitted to the cluster
6. You can go to `http://localhost:4040/` to see the live progress of your application.

## How to debug your code in IDE

1. Use IntelliJ Idea with Scala plug-in or any other Scala IDE.
2. The main code is in src/main/scala/cpts415/driver.scala
3. Uncomment ```.master("local[*]")``` after ```.config("spark.some.config.option", "some-value")``` to tell IDE the master IP is localhost.
4. Run your code in IDE
5. Once you finish your code, **comment out** ```.master("local[*]")``` again and follow the steps in "How to submit your code to Spark cluster"