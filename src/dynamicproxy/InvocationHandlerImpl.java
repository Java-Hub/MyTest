package dynamicproxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

public class InvocationHandlerImpl implements InvocationHandler {

	MyInterface myInterface;
	
	public InvocationHandlerImpl(MyInterface myInterface) {
		this.myInterface = myInterface;
	}
	
	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		return method.invoke(myInterface, args);
	}

}
