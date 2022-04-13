package com.epam.cdp.m2.hw2.aggregator;

import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.RecursiveTask;

import javafx.util.Pair;
import org.jetbrains.annotations.NotNull;

public class Java7ParallelAggregator implements Aggregator{

    static int numThreads = Runtime.getRuntime().availableProcessors();

    @Override
    public int sum(List<Integer> numbers) {
        int thresHold = numbers.size() / numThreads;
        ForkJoinPool pool = new ForkJoinPool(numThreads);
        return pool.invoke(new SumTask(numbers, thresHold));

    }

    class SumTask extends RecursiveTask<Integer> {
        int length;
        List<Integer> list;
        int thresHold;

        public SumTask(List<Integer> list, int thresHold) {
            this.list = list;
            this.thresHold = thresHold;
            this.length = list.size();
        }

        @Override
        protected Integer compute() {
            if (list.isEmpty()){
                return 0;
            }
            else if (length <= ((length < numThreads) ? numThreads : thresHold)) {
                int sum = 0;
                for (int num : list) {
                    sum += num;
                }
                return sum;
            }
            else {
                int mid = list.size() / 2;
                List<Integer> firstList = list.subList(0, mid);
                SumTask f1 = new SumTask(firstList, thresHold);
                f1.fork();
                List<Integer> secondList = list.subList(mid, length);
                SumTask f2 = new SumTask(secondList, thresHold);
                int computedValue = f2.compute();
                return f1.join() + computedValue;
            }
        }
    }


    @Override
    public List<Pair<String, Long>> getMostFrequentWords(List<String> words, long limit) {

        long counter = 0;
        ForkJoinPool pool = new ForkJoinPool(numThreads);
        Map<String, Long> entryMap = pool.invoke(new PairTask(words, words.size() / numThreads));
        List<Map.Entry<String, Long>> entryList = new LinkedList<>(entryMap.entrySet());
        Collections.sort(entryList, new Comparator<Map.Entry<String, Long>>() {
            @Override
            public int compare(Map.Entry<String, Long> o2,
                               Map.Entry<String, Long> o1) {
                return o1.getValue().compareTo(o2.getValue());
            }
        });

        List<Pair<String, Long>> sortedList = new ArrayList<>();
        for (Map.Entry<String, Long> entry : entryList) {
            if (counter >= limit) break;
            counter++;
            sortedList.add(new Pair<>(entry.getKey(), entry.getValue()));
        }

        return sortedList;
    }


    class PairTask extends RecursiveTask<Map<String, Long>> {

        private List<String> forkList, sortedList;
        private int threshold;
        private int size;


        public PairTask(List<String> forkList, int threshold) {
            this.forkList = forkList;
            this.threshold = threshold;
            this.size = forkList.size();
        }

        @Override
        protected Map<String, Long> compute() {
            if (size <= ((size < numThreads) ? numThreads : threshold)) {
                Map<String, Long> freqMap = new TreeMap<>();
                for (String word : forkList) {
                    freqMap.put(word, freqMap.containsKey(word) ? freqMap.get(word) + 1 : 1);
                }
                return freqMap;
            }
            else {
                int mid = size / 2;
                PairTask p1 = new PairTask(forkList.subList(0, mid), threshold);
                p1.fork();
                PairTask p2 = new PairTask(forkList.subList(mid, size), threshold);
                return mergeTree(p1.join(), p2.compute());
            }
        }

        private Map<String, Long> mergeTree(Map<String, Long> mapOne, Map<String, Long> mapTwo) {
            Map<String, Long> mergeMap = (isShorter(mapOne, mapTwo)) ? mapTwo : mapOne;
            Map<String, Long> shorter = (isShorter(mapOne, mapTwo)) ? mapOne : mapTwo;
            for (Map.Entry<String, Long> entry : shorter.entrySet()) {
                String key = entry.getKey();
                long val = entry.getValue();
                mergeMap.put(key, (mergeMap.containsKey(key) ? mergeMap.get(key) + val : val));
            }
            return mergeMap;
        }

        private boolean isShorter(@NotNull Map<String, Long> mA, @NotNull Map<String, Long> mB) {
            return mA.size() < mB.size();
        }


    }

    @Override
    public List<String> getDuplicates(List<String> words, long limit) {
        long counter = 0;
        ForkJoinPool pool = new ForkJoinPool(numThreads);
        Map<String, Long> entryMap = pool.invoke(new DuplicateTask(words, words.size() / numThreads));

        List<Map.Entry<String, Long>> entryList = new LinkedList<>(entryMap.entrySet());
        Collections.sort(entryList, new Comparator<Map.Entry<String, Long>>() {
            @Override
            public int compare(Map.Entry<String, Long> o1,
                               Map.Entry<String, Long> o2) {
                if(o1.getKey().length()>o2.getKey().length()){
                    return 1;
                }
                else if(o1.getKey().length()< o2.getKey().length()){
                    return -1;
                }
                return o1.getKey().compareTo(o2.getKey());
            }
        });

        List<String> sortedList = new ArrayList<>();
        for (Map.Entry<String, Long> entry : entryList) {
            if (counter >= limit) break;
            counter++;
            sortedList.add(entry.getKey());
        }

        return sortedList;


    }

    class DuplicateTask extends RecursiveTask<Map<String, Long>> {

        private List<String> forkList, sortedList;
        private int threshold;
        private int size;

        public DuplicateTask(List<String> forkList, int threshold) {
            this.forkList = forkList;
            this.threshold = threshold;
            this.size = forkList.size();
        }

        @Override
        protected Map<String, Long> compute() {
            final Map<String, Long> resultMap;
            if (size <= ((size < numThreads) ? numThreads : threshold)) {
                resultMap = getDuplicateMap(forkList);
            }
            else {
                int mid = size / 2;
                DuplicateTask p1 = new DuplicateTask(forkList.subList(0, mid), threshold);
                p1.fork();
                DuplicateTask p2 = new DuplicateTask(forkList.subList(mid, size), threshold);
                resultMap = mergeTree(p1.join(), p2.compute());
            }
            return resultMap;
        }

        //Merge map data
        private Map<String, Long> mergeTree(Map<String, Long> mapOne, Map<String, Long> mapTwo) {
            Map<String, Long> mergeMap = (isShorter(mapOne, mapTwo)) ? mapTwo : mapOne;
            Map<String, Long> shorter = (isShorter(mapOne, mapTwo)) ? mapOne : mapTwo;
            for (Map.Entry<String, Long> entry : shorter.entrySet()) {
                mergeMap.put(entry.getKey(), entry.getValue());
            }
            return mergeMap;
        }

        //shorter map
        private boolean isShorter(@NotNull Map<String, Long> firstMap, @NotNull Map<String, Long> secondMap) {
            return firstMap.size() < secondMap.size();
        }

        private Map<String, Long> getDuplicateMap(@NotNull final List<String> words) {
            Set duplicateSet  = new HashSet();
            Map<String, Long> lengthMap = new TreeMap<>();
            for (String word : words) {
                if (!duplicateSet.add(word.toUpperCase()))
                    lengthMap.put(word.toUpperCase(), Integer.toUnsignedLong(word.length()));
            }
            return lengthMap;
        }

    }

}
