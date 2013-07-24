# jcascalog-trevni-example

JCascalog Example with Trevni Data Format.

## Run Examples

### Write Trevni from Json
This example M/R Job output produces trevni data from the input json data.

Edit the namenode and jobtracker configuration in avro.trevni.playground.WriteTrevniFromJson.main():
```
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "hdfs://hadoop01:9000");
    conf.set("mapred.job.tracker", "hadoop01:9001");
```
   
To run WriteTrevniFromJson M/R Job:

```
mvn -e -DskipTests=true clean install assembly:single ;
hadoop fs -mkdir trevni;
hadoop fs -mkdir trevni/lib;
hadoop fs -mkdir trevni/json;
hadoop fs -put target/trevni-playground-0.1.0-SNAPSHOT-hadoop-job.jar trevni/lib;
hadoop fs -put src/test/resources/electricPowerUsageGenerator.json trevni/json/;
```

```
hadoop jar target/trevni-playground-0.1.0-SNAPSHOT-hadoop-job.jar avro.trevni.playground.WriteTrevniFromJson \
          trevni/json/electricPowerUsageGenerator.json  trevni/out \
          trevni/lib/trevni-playground-0.1.0-SNAPSHOT-hadoop-job.jar snappy;
```

### Read Specified Columns from Trevni data with raw M/R Job.
With this raw M/R Job, the specified column will be read from the trevni data.

Edit the namenode and jobtracker configuration in avro.trevni.playground.ReadSpecifiedColumns.main():
```
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "hdfs://hadoop01:9000");
    conf.set("mapred.job.tracker", "hadoop01:9001");
```
   
To run this M/R Job:

```
hadoop jar target/trevni-playground-0.1.0-SNAPSHOT-hadoop-job.jar avro.trevni.playground.ReadSpecifiedColumns \
        trevni/out/*  trevni/specified-columns \
        trevni/lib/trevni-playground-0.1.0-SNAPSHOT-hadoop-job.jar snappy;
```

