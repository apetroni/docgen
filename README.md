# docgen

Simple Apache Flink application written in Java.

## What it does

`SimpleFlinkApp` builds a tiny streaming pipeline from an in-memory list of strings:

1. Splits each line into words
2. Emits `(word, 1)` tuples
3. Groups by word and sums counts
4. Prints results to stdout
###<<<<<<< codex/create-simple-flink-application-in-java-ib3inn
5. Registers a checkpoint listener that writes checkpoint/job stats to `job-statistics.txt`

The checkpoint listener appends entries in this format:

```text
<timestamp> checkpointId=<id> status=<COMPLETED|ABORTED> processedRecords=<count> successfulCheckpoints=<count>
```


## Build

```bash
mvn package
```

## Run locally

```bash
java -jar target/docgen-flink-app-1.0-SNAPSHOT.jar
```
#### <<<<<<< codex/create-simple-flink-application-in-java-ib3inn

After the job runs, inspect `job-statistics.txt` for dumped checkpoint statistics.

