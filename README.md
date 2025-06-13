# Lightweight Java Database Engine

Lightweight Java database engine that to get hands-on with how storage, indexing, and query processing work under the hood. It’s all in Java, uses Apache Ant for builds and comes with JUnit tests.

## Features

- **Disk-Backed Buffer Manager**  
  A fixed-size buffer pool with LRU eviction, dirty-page tracking, and pin-counts to coordinate safe concurrent access. Pages are persisted to a binary file at fixed offsets (pageId × pageSize) :contentReference[oaicite:0]{index=0}.

- **B⁺-Tree Index**  
  A disk-based B⁺-tree layered on the buffer manager, supporting insertions, point queries, and range queries over serialized key–RID entries :contentReference[oaicite:1]{index=1}.

- **Pipelined Query Executor**  
  Implements Scan, Selection, Projection and Block Nested-Loop Join operators that stream tuples one at a time and materialize only as needed. Includes I/O cost estimation based on a derived block-nested-loop formula .

- **IMDb Dataset Support**  
  Preprocessors and loaders for IMDb TSV files (`title.basics`, `title.principals`, `name.basics`) so you can benchmark real data.

- **Automated Build & Tests**  
  Apache Ant build scripts, JUnit tests for all components, and example shell wrappers for running end-to-end queries.

---

## Prerequisites

- **Java 8+** (JDK)  
- **Apache Ant 1.10+**  
- **JUnit 4** (`junit-4.13.2.jar`, `hamcrest-core-1.3.jar`)

---

## Getting Started

1. **Clone the repository**

2. **Prepare the data**

   ```bash
   mkdir data
   curl -L -o data/title.basics.tsv.gz   https://datasets.imdbws.com/title.basics.tsv.gz
   curl -L -o data/title.principals.tsv.gz https://datasets.imdbws.com/title.principals.tsv.gz
   curl -L -o data/name.basics.tsv.gz     https://datasets.imdbws.com/name.basics.tsv.gz
   gunzip data/*.gz
   ```

3. **Build & run the storage manager**

   ```bash
   cd buffer-manager
   ant run     # loads title.basics into imdb.bin, demonstrating page eviction
   ant test    # runs JUnit tests
   cd ..
   ```

4. **Build & run the indexing engine**

   ```bash
   cd tree-index
   ant run     # inserts & queries a B⁺-tree over the movie titles
   cd ..
   ```

5. **Load data & execute queries**

   ```bash
   cd query-execution
   ant load-imdb             # preprocess all three tables into binary pages
   ./src/query.sh 3          # run predefined query #3 (defaults to buffer size 200)
   # or via Ant:
   ant query-imdb -Dargs="'Close Encounters' 'Closer' 200"
   ```

---

## Project Structure

```
/
├── buffer-manager/        # Storage manager (LRU buffer pool + binary persistence)
│   ├── src/               # Java code (BufferManager, Page, Row, Main)
│   ├── data/              # title.basics.tsv → imdb.bin
│   └── build.xml          # Ant script & JUnit config
│
├── tree-index/            # B⁺-tree implementation over buffer-manager
│   ├── src/               # Java code (BPlusTree, TabularPage)
│   └── build.xml          # Ant script
│
├── query-execution/       # Pipelined query executor & cost estimator
│   ├── src/               # Java code (Operators, LoadIMDb, RunIMDbQuery)
│   ├── query.sh           # Example shell wrapper for running queries
│   └── build.xml          # Ant script
│
├── data/                  # Raw IMDb TSV files (shared across modules)
└── LICENSE                # Apache 2.0 (example)
```
---

## Contributors
- Evan Ciccarelli (eciccarelli0016)
- Matthew Richardson (06tron)
- Priyanka Gupta (priyankagpta)
- Tanvi Agarwal (tanvi217)