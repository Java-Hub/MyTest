package main.java.stream;

import java.math.BigInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StreamIterate {

	public static void main(String[] args) {
		Stream<String> stream = Stream.iterate(BigInteger.ZERO, bigInteger -> bigInteger.add(BigInteger.ONE)).limit(1000).map(bigInteger -> bigInteger.toString());
		String collect = stream.collect(Collectors.joining("-"));
		System.out.println(collect);
	}

}
