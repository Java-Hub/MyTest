package main.java.stream;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StreamIterate {

	public static void main(String[] args) {
		Stream<String> stream = Stream.of("1", "2", "3", "4", "5");
		String val = stream.collect(Collectors.joining("-"));
		System.out.println(val);
	}

}
