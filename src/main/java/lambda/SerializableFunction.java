package main.java.lambda;

import java.io.Serializable;

@FunctionalInterface
public interface SerializableFunction extends Serializable {

	void print(Object obj);
	
}
