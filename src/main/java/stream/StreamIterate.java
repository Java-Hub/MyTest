package main.java.stream;

import java.math.BigInteger;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

public class StreamIterate {

    public static void main(String[] args) {
        Stream<BigInteger> stream = Stream.iterate(BigInteger.ZERO, bigInteger -> bigInteger.add(BigInteger.ONE));
        stream.forEach(System.out::println);
    }

}
