# docgen

Simple Apache Flink application written in Java.

## What it does

`SimpleFlinkApp` builds a tiny streaming pipeline from an in-memory list of strings:

1. Splits each line into words
2. Emits `(word, 1)` tuples
3. Groups by word and sums counts
4. Prints results to stdout

## Build

```bash
mvn package
```

## Run locally

```bash
java -jar target/docgen-flink-app-1.0-SNAPSHOT.jar
```
