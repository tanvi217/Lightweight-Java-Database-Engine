# tree-index

## Lab 2 Submission

1. [Written Report](https://docs.google.com/document/d/1SG5lAwYw57gmQXd6RDFmwVrRFUDq0thglfQiCLqKAug/edit)
2. To run the code submitted on April 7th, start by cloning the repository. Switch to the commit specified below as changes may have been made since submission.
```sh
git clone https://github.com/06tron/cs645-labs.git
cd cs645-labs
git checkout "TO BE ADDED"
```
3. Move the IMDb TSV file into the `data` subdirectory of `cs645-labs`.
```sh
mkdir data
curl --output-dir data -O https://datasets.imdbws.com/title.basics.tsv.gz
gunzip data/title.basics.tsv.gz
```
4. Build and run the code using Apache Ant 1.10.15 and Java. The `ant run` command runs the main method of the `tree-index/src/CreateIndex.java` file.
```sh
cd tree-index
ant run
```
