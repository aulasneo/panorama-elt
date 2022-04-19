# panorama-extract-load
Tools to extract data and load to the datalake for Panorama analytics.

## Requirements
Requires Python 3.5 or newer and virtualenv installed.
`make install` will create a new virtual environment and install all dependencies 
listed in `requirements.txt` 
## Installation
Run
```shell
make install
```

## Configuration

1. Copy `openedx_extract_and_load_example.sh` to a new file.
2. Edit the script and update the variables with appropriate values
3. Run the script

## Setting up the cronjob

To run the script once an hour, add a line similar to the following one to the crontab:

```shell
0 * * * * cd panorama-extract-load && venv/bin/python panorama.py >> panorama.log

```