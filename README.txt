Task description

Your main goal is to implement Aggregator interface. There are already four classes implementing the interface, but all their methods throw UnsupportedOperationException:

1.	Java7Aggregator – should use Java 7 approach to solve the problem (no packages from Java 8)
2.	Java8Aggregator – should use Java 8 Stream API to solve the problem
3.	Java8ParallelAggregator – should use parallel streams from Java 8 Stream API to solve the problem
4.	Java7ParallelAggregator -  use Java 7 approach to do parallel tasks (e.g. you may use fork/join framework) to solve the problem

How to run tests

- Use 'com.epam.cdp.m2.hw2.aggregator.suits' package to run test by implementation;
- Use classes inside 'com.epam.cdp.m2.hw2.aggregator.[sum|duplicates|frequency].impl' package to test each separate method of each separate implementation.

