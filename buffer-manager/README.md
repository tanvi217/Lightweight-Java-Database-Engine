# buffer-manager

TODO make note of new changes

## Lab 1 Submission

1. [Written Report](https://docs.google.com/document/d/1yRNIZFOOBZGDW5Cv4G_jT15FkewFEATbQRjvpLWB9GQ/edit)
2. To run the code submitted on March 9th, start by cloning the repository. Switch to the commit specified below as changes have been made since submission.
```sh
git clone https://github.com/06tron/cs645-labs.git
cd cs645-labs
git checkout febbee43c6dc42044b91f8c67e34f5b84e88f24c
mkdir lib buffer-manager/data
```
3. Move the IMDb TSV file into the `buffer-manager/data` subdirectory of `cs645-labs`. Do the same for JUnit 4.13.2 and Hamcrest Core 1.3, placing them into the `lib` subdirectory.
```sh
mkdir buffer-manager/data lib
curl --output-dir buffer-manager/data -O https://datasets.imdbws.com/title.basics.tsv.gz
curl --output-dir lib -O https://repo1.maven.org/maven2/junit/junit/4.13.2/junit-4.13.2.jar
curl --output-dir lib -O https://repo1.maven.org/maven2/org/hamcrest/hamcrest-core/1.3/hamcrest-core-1.3.jar
```
4. Build and run the code using Apache Ant 1.10.15 and Java. After running the test command, text files containing the output will be created in the `test` directory.
```sh
cd buffer-manager
ant test
ant run
```
