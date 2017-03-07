# Benchmarks

Benchmarks are based on [JMH](http://openjdk.java.net/projects/code-tools/jmh/).

# Running Benchmarks

Running benchmarks is disabled by default and can be activated via the `benchmarks` profile.
To run the benchmarks with default settings use.

```bash
mvn -P benchmarks clean test
```

A basic report will be printed to the CLI.

```bash
# Run complete. Total time: 00:00:15

Benchmark                                    Mode  Cnt        Score       Error  Units
MappingMongoConverterBenchmark.readObject   thrpt   10  1920157,631 ± 64310,809  ops/s
MappingMongoConverterBenchmark.writeObject  thrpt   10   782732,857 ± 53804,130  ops/s
```

## Running all Benchmarks of a specific class

To run all Benchmarks of a specific class, just provide its simple class name via the `benchmark` command line argument.

```bash
mvn -P benchmarks clean test -D benchmark=MappingMongoConverterBenchmark
```

## Running a single Benchmark

To run a single Benchmark provide its containing class simple name followed by `#` and the method name via the `benchmark` command line argument.

```bash
mvn -P benchmarks clean test -D benchmark=MappingMongoConverterBenchmark#readObjectWith2Properties
```

# Saving Benchmark Results

A detailed benchmark report is stored in JSON format in the `/target/reports/performance` directory.
To store the report in a different location use the `benchmarkReportDir` command line argument.

## MongoDB

Results can be directly piped to MongoDB by providing a valid [Connection String](https://docs.mongodb.com/manual/reference/connection-string/) via the `publishTo` command line argument.

```bash
mvn -P benchmarks clean test -D publishTo=mongodb://127.0.0.1:27017
```

NOTE: If the uri does not explicitly define a database the default `spring-data-mongodb-benchmarks` is used. 

## HTTP Endpoint

The benchmark report can also be posted as `application/json` to an HTTP Endpoint by providing a valid URl via the `publishTo` command line argument.

```bash
mvn -P benchmarks clean test -D publishTo=http://127.0.0.1:8080/capture-benchmarks
```

# Customizing Benchmarks

Following options can be set via command line.

Option | Default Value
--- | ---
warmupIterations | 10
warmupTime | 1 (seconds)
measurementIterations | 10
measurementTime | 1 (seconds)
forks | 1
benchmarkReportDir | /target/reports/performance (always relative to project root dir)
benchmark | .* (single benchmark via `classname#benchmark`)
publishTo | \[not set\] (mongodb-uri or http-endpoint)