package main.java.jvm;

import java.lang.reflect.Field;

public class Decompiler {

	public static void decompiler(Class<?> clazz) {
		String name = clazz.getName();
		System.out.println("name:" + name);
		Field[] fields = clazz.getDeclaredFields();
		for (Field field : fields) {
			System.out.print(field.getDeclaredAnnotations());
			System.out.print(" ");
			System.out.print(field.getType().getTypeName());
			System.out.print(" ");
			System.out.print(field.getName());
		}
	}

}
