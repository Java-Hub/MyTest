package main.java.jvm;

public class TestSB {

	public static void main(String[] args) {
		long start = System.currentTimeMillis();
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < 100000; i++) {
			builder.append(i).append("+").append(i).append("=").append(i + i).append("\n");
		}
		System.out.println(builder.length());
		long end = System.currentTimeMillis();
		System.out.println(end - start);

		start = System.currentTimeMillis();
		StringBuffer buffer = new StringBuffer();
		for (int i = 0; i < 100000; i++) {
			buffer.append(i).append("+").append(i).append("=").append(i + i).append("\n");
		}
		System.out.println(buffer.length());
		end = System.currentTimeMillis();
		System.out.println(end - start);
	}

}
