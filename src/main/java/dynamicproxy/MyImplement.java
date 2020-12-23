package main.java.dynamicproxy;

public class MyImplement implements MyInterface {

	String something;
	
	@Override
	public void doSomething(String something) {
		System.out.println("doSomething:" + something);
	}

	@Override
	public void setSomething(String something) {
		this.something = something;
	}

	@Override
	public void doSomething() {
		doSomething(something);
	}

}
