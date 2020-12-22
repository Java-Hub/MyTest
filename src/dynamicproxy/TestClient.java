package dynamicproxy;

import java.lang.reflect.Proxy;

public class TestClient {

	public static void main(String[] args) {
		MyImplement implement = new MyImplement();
		InvocationHandlerImpl handler = new InvocationHandlerImpl(implement);
		MyInterface myInterface = (MyInterface)Proxy.newProxyInstance(implement.getClass().getClassLoader(), implement.getClass().getInterfaces(), handler);
		myInterface.doSomething("hello");
		myInterface.setSomething("哈哈哈");
		myInterface.doSomething();
		implement.doSomething();
		
		implement.setSomething("1111");
		implement.doSomething();
		myInterface.doSomething();
	}

}
