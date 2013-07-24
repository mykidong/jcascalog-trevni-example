# jcascalog-trevni-example

JCascalog Example with Trevni Data Format.

## Run Examples

### Write Trevni from Json
Edit the namenode and jobtracker configuration in avro.trevni.playground.WriteTrevniFromJson.main().

To run WriteTrevniFromJson M/R Job:

```
mvn -e -DskipTests=true clean install assembly:single ;
hadoop fs -mkdir trevni;
hadoop fs -mkdir trevni/lib;
hadoop fs -put target/trevni-playground-0.1.0-SNAPSHOT-hadoop-job.jar trevni/lib;
hadoop fs -put src/test/resources/electricPowerUsageGenerator.json trevni/json/;
```

```
hadoop jar target/trevni-playground-0.1.0-SNAPSHOT-hadoop-job.jar avro.trevni.playground.WriteTrevniFromJson \
          trevni/json/electricPowerUsageGenerator.json  trevni/out \ 
          trevni/lib/trevni-playground-0.1.0-SNAPSHOT-hadoop-job.jar snappy;
```

