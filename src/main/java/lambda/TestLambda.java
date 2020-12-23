package main.java.lambda;

public class TestLambda {

	public static void main(String[] args) throws Exception {
		MyClass myClass = new MyClass();
		myClass.print(myClass::print);
	}

}

class MyClass{
	String name = "MyClass";
	void print(SerializableFunction func) {
		Class<? extends SerializableFunction> class1 = func.getClass();
		System.out.println(class1);
		func.print(name);
	}
	
	void print(Object info) {
		System.out.println(info);
	}
	
}
