# cs645-labs

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

### Prerequisites
- Java Development Kit (JDK)
- Apache Ant

### Steps to Set Up the Repository

1. **Clone the Repository**
   ```sh
   git clone https://github.com/your-repo/cs645-labs.git
   cd cs645-labs
   ```

2. **Download Dependencies**
Download the JUnit 4.13.2 and Hamcrest Core 1.3 JAR files and place them in the `lib` directory:
   - [junit-4.13.2.jar](https://repo1.maven.org/maven2/junit/junit/4.13.2/junit-4.13.2.jar)
   - [hamcrest-core-1.3.jar](https://repo1.maven.org/maven2/org/hamcrest/hamcrest-core/1.3/hamcrest-core-1.3.jar)

3. **Build & Run**
    ```sh
    ant run
    ```

### Directory Structure

The directory structure of the project is as follows:

```
cs645-labs
├── build.xml
├── lib
│   ├── hamcrest-core-1.3.jar
│   └── junit-4.13.2.jar
├── src
│   ├── BufferManagerLRU.java
│   ├── Constants.java
│   └── Main.java
|   ...
└── test
    └── BufferManagerLRUTest.java
    ...
```