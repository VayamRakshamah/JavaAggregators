package com.epam.cdp.m2.hw2.aggregator;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import javafx.util.Pair;

public class Java7Aggregator implements Aggregator {

    @Override
    public int sum(List<Integer> numbers) {
        int sum  = 0;
        for(Integer num : numbers){
            sum+=num;
        }
        return sum;
    }

    @Override
    public List<Pair<String, Long>> getMostFrequentWords(List<String> words, long limit) {
        Map<String,Long> collectmap = new HashMap<>();
        for(String word : words){
            collectmap.merge(word,1L,Long::sum);
        }
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
        Set<String> checkSet = new HashSet<>();
        for (String word : words) {
            if (!checkSet.add(word.toUpperCase())) {
                duplicates.add(word.toUpperCase());
            }
        }
        int n = duplicates.size();
        List<String> final_list = new ArrayList<>(n);
        for(String str:duplicates){
            final_list.add(str);
        }
        Collections.sort(final_list, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                if(o1.length()>o2.length()){
                    return 1;
                }
                // sort line updated
                else if(o1.length()<o2.length()){
                    return -1;
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
