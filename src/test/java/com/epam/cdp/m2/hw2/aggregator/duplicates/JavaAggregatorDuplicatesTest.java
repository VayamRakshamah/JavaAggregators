package com.epam.cdp.m2.hw2.aggregator.duplicates;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runners.Parameterized;

import com.epam.cdp.m2.hw2.aggregator.Aggregator;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

public abstract class JavaAggregatorDuplicatesTest {

    @Parameterized.Parameter(0)
    public long limit;

    @Parameterized.Parameter(1)
    public List<String> words;

    @Parameterized.Parameter(2)
    public List<String> expected;

    private Aggregator aggregator;

    public JavaAggregatorDuplicatesTest(Aggregator aggregator) {
        this.aggregator = aggregator;
    }

    @Parameterized.Parameters
    public static List<Object[]> data() {
        List<Object[]> data = new ArrayList<>();
        data.add(new Object[]{3, asList("wwwoooo", "a", "BA", "wwwoOoo", "ba"), asList("BA", "WWWOOOO")});
        data.add(new Object[]{3, asList("all", "arguments", "a", "map", "wwwwwww", "map", "arguments", "all"), asList("ALL", "MAP", "ARGUMENTS")});
        data.add(new Object[]{2, asList("dog", "word", "intellij", "a", "b", "dog", "word", "intellij"), asList("DOG", "WORD")});
        data.add(new Object[]{3, Collections.emptyList(), Collections.emptyList()});
        data.add(new Object[]{3, asList("www"), Collections.emptyList()});
        return data;
    }

    @Test
    public void test() {
        long start = System.nanoTime();
        List<String> actual = aggregator.getDuplicates(words, limit);
        long end = System.nanoTime();
        assertEquals(expected, actual);
        System.out.println("Duplicate RunTime : "+(end-start));
    }
}