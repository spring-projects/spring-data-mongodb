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

More detailed information is stored in JSON format in the `/target/reports/performance` directory.

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