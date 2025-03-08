import java.util.function.Supplier;

public class Test {

    public static BufferManager getNewBM(int bufferSize) {
        // change to whichever BufferManager extension you want to test
        return new LRUBufferManager(bufferSize);
    }

    public static void main(String[] args) {

        // test cases, each has no arguments and returns a string result
        @SuppressWarnings("unchecked")
        Supplier<String>[] tests = new Supplier[] {
                BufferManagerTest::overfillBuffer,
                BufferManagerTest::evictPage,
                BufferManagerTest::lruLongerTest,
                IMDbTest::datasetTest,
                // add new test cases here
        };

        for (int i = 0; i < tests.length; ++i) {
            System.out.println("TEST " + (i + 1) + " RESULT: " + tests[i].get());
        }

    }

}
