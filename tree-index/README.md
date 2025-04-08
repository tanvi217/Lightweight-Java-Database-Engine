# tree-index

## Lab 2 Submission

1. [Written Report](https://docs.google.com/document/d/1SG5lAwYw57gmQXd6RDFmwVrRFUDq0thglfQiCLqKAug/edit)
2. To run the code submitted on April 7th, start by cloning the repository. Switch to the commit specified below as changes have been made since submission.
```sh
git clone https://github.com/06tron/cs645-labs.git
cd cs645-labs
git checkout "TO BE ADDED"
```
3. Move the IMDb TSV file into the `data` subdirectory of `cs645-labs`.
```sh
mkdir data
curl --output-dir data -O https://datasets.imdbws.com/title.basics.tsv.gz
```
4. Build and run the code using Apache Ant 1.10.15 and Java.
```sh
cd buffer-manager
ant run "SOMETHING"
```
