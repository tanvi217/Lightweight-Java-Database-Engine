# buffer-manager

Move the IMDb TSV file into the `buffer-manager/data` subdirectory. Do the same for JUnit 4.13.2 and Hamcrest Core 1.3, placing them into the `lib` subdirectory.
```sh
mkdir buffer-manager/data lib
curl --output-dir buffer-manager/data -O https://datasets.imdbws.com/title.basics.tsv.gz
gunzip buffer-manager/data/title.basics.tsv.gz
curl --output-dir lib -O https://repo1.maven.org/maven2/junit/junit/4.13.2/junit-4.13.2.jar
curl --output-dir lib -O https://repo1.maven.org/maven2/org/hamcrest/hamcrest-core/1.3/hamcrest-core-1.3.jar
```
Build and run the code using Apache Ant 1.10.15 and Java. After running the test command, text files containing the output will be created in the `test` directory.
```sh
cd buffer-manager
ant test
ant run
```
