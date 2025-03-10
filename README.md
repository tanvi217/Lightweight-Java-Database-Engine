# cs645-labs

### Steps to Set Up the Repository

1. **Clone the Repository**
   ```sh
   git clone https://github.com/06tron/cs645-labs.git
   cd cs645-labs
   ```
2. Download the JUnit 4.13.2 and Hamcrest Core 1.3 JAR files and place them in the `lib` subdirectory of `cs645-labs`:
   - [junit-4.13.2.jar](https://repo1.maven.org/maven2/junit/junit/4.13.2/junit-4.13.2.jar)
   - [hamcrest-core-1.3.jar](https://repo1.maven.org/maven2/org/hamcrest/hamcrest-core/1.3/hamcrest-core-1.3.jar)

3. Download the IMDb TSV file and place it into the `buffer-manager/data` directory:
   - [title.basics.tsv](https://datasets.imdbws.com/title.basics.tsv.gz)

4. Prerequisites
   - Java Development Kit (JDK)
   - Apache Ant
  
6. **Build & Run**
    ```sh
    cd buffer-manager
    ant
    ```

7. To test the code, run `ant test`. Text files for the test output will be created in the `test` directory.

### Directory Structure

The directory structure of the project is as follows:

```
cs645-labs
├── lib
│   ├── hamcrest-core-1.3.jar
│   └── junit-4.13.2.jar
└── buffer-manager
    ├── src
    │   ├── BufferManager.java
    │   ├── Page.java
    │   └── ...
    ├── test
    │   ├── TEST-BufferManagerTest.txt
    │   ├── BufferManagerTest.java
    │   └── ...
    ├── data 
    │   └── title.basics.tsv
    └── build.xml

```

### group members
- Priyanka Gupta (priyankagpta)
- Evan Ciccarelli (eciccarelli0016)
- Tanvi Agarwal (tanviagarwal15)
- Matthew Richardson (06tron)

### teaching and course assistants
- Sandeep Polisetty (sandeep06011991)
- Hasnain Heickal (hheickal)
- Alex Zheng (alex-haozheng)
- Harshini Bharanidharan (hbharanidhar)
- Meghana Krishnan (meghanakrish19)
- Dhruv Maheshwari (dhruvm2509)
- Rachna Ajit Soundatti (rachnasoundatti)

## resources

[Apache Ant Manual](https://ant.apache.org/manual/using.html)
[Report](https://docs.google.com/document/d/1yRNIZFOOBZGDW5Cv4G_jT15FkewFEATbQRjvpLWB9GQ/edit?tab=t.0)
