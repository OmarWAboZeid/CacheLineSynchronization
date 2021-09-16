package com.company;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


public class Main {

    public static void main(String[] args) throws InterruptedException, FileNotFoundException {

        for (int numThreads : new int[] {1,2,4,6,8,10,12,14,16}) {
            int maxTotalSize=(int) (1.2e8);

            int numRuns = 20;


            PrintWriter out = new PrintWriter("Experiment"+numThreads+".csv");

            out.println("Total Size,Sequential,Seperate Memory");

            for (long totalSize = 120; totalSize <= maxTotalSize; totalSize *= 10) {
                long totalSequential = 0;
                long totalSeperateMemory = 0;
                long numInsertions = totalSize / numThreads;
                for (int run = 0; run < numRuns; run++) {

                    long sequential = testCacheLine(numThreads, numInsertions, true);
                    long seperateMemory = testCacheLine(numThreads, numInsertions, false);
                    totalSequential += sequential;
                    totalSeperateMemory += seperateMemory;
                }


                out.println(totalSize + "," + totalSequential*1.0/numRuns + "," + totalSeperateMemory*1.0/numRuns);
            }
            out.close();
        }




    }

    static long testCacheLine(int numThreads, long numInsertions, boolean isSequential) throws InterruptedException {
//        Array dictionary = Array.create(Type.Long, "test");
        long totalSize = numThreads * 1L * numInsertions;
        Long[] dictionary = new Long[Math.toIntExact(totalSize)];
//        dictionary.setSize(totalSize);

        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

        List<Callable<Object>> tasks = new ArrayList<>();

        AtomicLong index = isSequential? new AtomicLong() : null;

        for (int i = 0; i < numThreads; i++) {
            long startIndex = i * numInsertions;

            Runnable runnable = isSequential ? getRunner(index, numInsertions, dictionary) : getRunner(startIndex, numInsertions, dictionary);

            tasks.add(Executors.callable(runnable));
        }

        long startTime = System.currentTimeMillis();

        executorService.invokeAll(tasks);

        long endTime = System.currentTimeMillis();

        executorService.shutdown();

//        shutdownAndAwaitTermination(executorService);

//        if (!verify(dictionary, totalSize)) {
//            dictionary.clear();
//            return -1;
//        }
//        dictionary.clear();
        dictionary = null;
        return endTime - startTime;

    }


    static void shutdownAndAwaitTermination(ExecutorService pool) {
        pool.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!pool.awaitTermination(2, TimeUnit.MINUTES)) {
                pool.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!pool.awaitTermination(60, TimeUnit.SECONDS))
                    System.err.println("Pool did not terminate");
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            pool.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }

    static Runnable getRunner(AtomicLong startIndex, long numInsertions, Long[] dictionary) {
        return () -> {
            Long index = startIndex.get();
            for (long j = 0; j < numInsertions; j++) {
                dictionary[(int) (index + j)] = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
            }
        };
    }

    static Runnable getRunner(long index, long numInsertions, Long[] dictionary) {
        return () -> {
            long value = index;
            for (long j = 0; j < numInsertions; j++) {
                dictionary[(int) value] = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
                value++;
            }
        };
    }
}
