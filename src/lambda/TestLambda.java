package lambda;

public class TestLambda {

	public static void main(String[] args) {
		MyClass myClass = new MyClass();
		myClass.print(System.out::print);
	}

}

class MyClass{
	String name = "MyClass";
	void print(SerializableFunction func) {
		func.print(name);
	}
}
