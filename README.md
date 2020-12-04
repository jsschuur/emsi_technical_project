# emsi_technical_project

The program was written in python3.

The purpose of this program is to stream the data from `sample.gz` and compile it into a sqlite database along with preforming a few tertiary calculations.

The entirety of the program can be found in `app.py`.

The database should not exist in the directory of the program prior to execution.

No external modules that would require installation were used.

## How to run
1. Open a terminal
2. Navigate to the directory that contains `app.py`
3. run `python3 app.py`

The number of occurences for each soc2, how many postings were active on Feb 1, 2017, and the number of documents for which html tags were removed will be printed to stdout, and a database named `output.db` will be created in the directory.
