
# Install

+ Clone this repo
+ `python3 -m venv env && source env/bin/active`
+ `pip install -r requirements.txt`


# Usage

```
python -m gouvapi ACTION --output OUTPUT [--input INPUT]
                  [--threads THREADS] [--increment INCREMENT]
Action:
    ACTION      'fetch' or 'process',  Action to perform

Parameters
    --output, -t       Output file
    --input, -i        Input file, only for 'process' action
    --increment, -I    Api's chuking size for 'fetch'
    --threads, -t      Number of threads
```


## Fetch, then process

This app is divided in two actions, `fetch` and `process`.

`fetch` produce a file required to run ̀ process`.


## Example

Fetch csv resource by calling API by chunk of 40 and 4 threads:

```
python -m gouvapi fetch --output /tmp/resources.txt -I 40 -t 4
```

Processing extracted resources urls with 16 threads:

```
python -m gouvapi process -o /tmp/data.json -i /tmp/resources.txt -t 16
```


# Output


## Parsed csv files

In provided output (̀ ./output/data.json`) 7201 csv files are parsed.

Only files that can be decoded and parsed are saved.


## Fetch output

`fetch` action product a csv ouput, this one list all extracted resources link from API.

An example is provided at `./output/resources.csv`


