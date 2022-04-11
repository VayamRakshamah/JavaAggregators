package com.epam.cdp.m2.hw2.aggregator;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import javafx.util.Pair;

public class Java8Aggregator implements Aggregator {

    @Override
    public int sum(List<Integer> numbers) {
        return numbers.stream().reduce(0,Integer::sum);
    }

    @Override
    public List<Pair<String, Long>> getMostFrequentWords(List<String> words, long limit) {
        Map<String,Long> collectmap = words.stream().collect(Collectors.groupingBy(Function.identity(),Collectors.counting()));
        List<Pair<String,Long>> nvpList = new ArrayList<>(collectmap.size());

        for (Map.Entry<String,Long> entry : collectmap.entrySet()) {
            nvpList.add(new Pair<String,Long>(entry.getKey(), entry.getValue()));
        }

        nvpList.sort(new Comparator<Pair<String, Long>>() {
            @Override
            public int compare(Pair<String, Long> o1, Pair<String, Long> o2) {
                if(o1.getValue() > o2.getValue()){
                    return -1;
                }
                else{
                    return o1.getKey().compareTo(o2.getKey());
                }
            }
        });

        int num = nvpList.size();
        if(num<(int)limit){
            return nvpList.subList(0,num);
        }
        else{
            return nvpList.subList(0,(int)limit);
        }
    }

    @Override
    public List<String> getDuplicates(List<String> words, long limit) {
        Set<String> duplicates = new HashSet<>();
        List<String> final_list = words.stream().map(String::toUpperCase).filter( n -> !duplicates.add(n)).collect(Collectors.toList());
        Collections.sort(final_list, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                if(o1.length()>o2.length()){
                    return 1;
                }
                else{
                    return o1.compareTo(o2);
                }
            }
        });
        int num = final_list.size();
        if(num<(int)limit){
            return final_list.subList(0,num);
        }
        else{
            return final_list.subList(0,(int)limit);
        }

    }
}