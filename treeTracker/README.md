To run the unit tests

```$xslt
$ mvn clean install -P stats -P dev
```

To benchmark performance

```
$ mvn clean install -DskipTests -P prod
```

To enable statistics information collection

```
$ mvn clean install -DskipTests -P stats -P dev
```

Note, for HJ, TTJ, TTJV2 statistics collection, instead of the 
above command, we run

```
$ mvn clean install -DskipTests -P stats
 ```

To enable tracing

```
$ mvn clean install -DskipTests -P dev
$ export treetracker_debug=1
```

| Benchmark | Performance    | Statistics                        |
|-----------|----------------|-----------------------------------|
| SSB       | `BenchmarkSSB` |`BenchmarkHvTSSB`                  |
| JOB       |                |`BenchmarkHvTJOBDifferentOrdering` |
| TPC-H     |                |`BenchmarkHvTTPCHDifferentOrdering`|
