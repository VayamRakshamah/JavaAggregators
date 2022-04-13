package com.epam.cdp.m2.hw2.aggregator.sum;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runners.Parameterized;

import com.epam.cdp.m2.hw2.aggregator.Aggregator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

public abstract class JavaAggregatorSumTest {
    @Parameterized.Parameter(0)
    public List<Integer> numbers;

    @Parameterized.Parameter(1)
    public int expected;

    private Aggregator aggregator;

    public JavaAggregatorSumTest(Aggregator aggregator) {
        this.aggregator = aggregator;
    }

    @Parameterized.Parameters
    public static List<Object[]> data() {
        List<Object[]> data = new ArrayList<>();
        data.add(new Object[]{asList(1, 2, 3, 4, 5, 6, 7, 8), 36});
        data.add(new Object[]{asList(10, -10, 3), 3});
        data.add(new Object[]{emptyList(), 0});
        return data;
    }

    @Test
    public void test() {
        long start = System.nanoTime();
        int actual = aggregator.sum(numbers);
        long end = System.nanoTime();
        assertEquals(expected, actual);
        System.out.println("Sum RunTime : "+(end-start));
    }
}