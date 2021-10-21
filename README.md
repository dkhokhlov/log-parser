# log-parser

Usage: log-parser.py [OPTIONS]


```
Options:
  --version                       Show the version and exit.
  --in FILE                       Input file.  CLF format. [required]
  --out FILE                      Output JSON file.  [required]
  --max-client-ips INTEGER RANGE  Maximum number of results in the
                                  <top_client_ips> field.  [default:
                                  10;0<=x<=10000]
  --max-paths INTEGER RANGE       Maximum number of results in the
                                  <top_path_avg_seconds> field.  [default:
                                  10;0<=x<=10000]
  --help                          Show this message and exit.
```

# dependencies
```
channels:
  - defaults
dependencies:
  - click
  - pip
  - statsd
```

 
  
