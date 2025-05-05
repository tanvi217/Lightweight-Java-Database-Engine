# query-execution

## Lab 3 Submission

1. [Written Report](https://docs.google.com/document/d/1j5KOT5JKmtH9RWCr999a_85-scG9NIJPKxF5hSJj3TY/edit)
2. To run the code submitted on May 4th, start by cloning the repository. Switch to the commit specified below as changes may have been made since submission.
```sh
git clone https://github.com/06tron/cs645-labs.git
cd cs645-labs
git checkout 510656707eb71864f0d60f638508da1867942cde
```
3. Move the IMDb TSV files into the `data` subdirectory of `cs645-labs`.
```sh
mkdir data
curl --output-dir data -O https://datasets.imdbws.com/title.basics.tsv.gz
curl --output-dir data -O https://datasets.imdbws.com/title.principals.tsv.gz
curl --output-dir data -O https://datasets.imdbws.com/name.basics.tsv.gz
gunzip data/title.basics.tsv.gz
gunzip data/title.principals.tsv.gz
gunzip data/name.basics.tsv.gz
```
4. Build and run the preprocess step using Apache Ant 1.10.15 and Java. The `ant load-imdb` command runs the main method of the `query-execution/src/LoadIMDb.java` file. This could take over a minute to run, and attempts to load 10 million lines from each TSV file. The number of rows loaded can be adjusted at the top of the LoadIMDb.java main method.
```sh
cd query-execution
ant load-imdb
```
4. Use the `query-execution/src/query.sh` shell script to run queries on the loaded data. There are 7 test queries already specified in the script file. These can be called by passing its number as a single argument. The argument for buffer size is optional and defaults to 200. If you cannot run the shell script, an Ant command taking all three arguments can be used instead. The four following commands are all equivalent.
```sh
./query.sh 'Close Encounters' Closer 200
./query.sh 'Close Encounters' Closer
./query.sh 3
ant query-imdb -Dargs="'Close Encounters' Closer 200"
```
