package main.java;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

public class TestJava {

	public static void main(String[] args) throws Exception {
		
		ScriptEngineManager manager = new ScriptEngineManager();
		ScriptEngine engine = manager.getEngineByName("scala");
		engine.eval("print(123)");
		
		
		
		
		
		
		
//		MyScala myScala = new MyScala();
//		myScala.setStr("MyScala");
//		List<String> list = new ArrayList<>();
//		list.add("aaa");
//		list.add("bbb");
//		list.add("ccc");
//		myScala.setList(list);
//		Bindings bindings = manager.getBindings();
//		bindings.put("myscala", myScala);
//		engine.eval(getScript(), bindings);
	}

	public static String getScript() throws IOException {
		try (InputStream is = TestJava.class.getResourceAsStream("/script")) {
			StringBuffer sb = new StringBuffer();
			byte[] bytes = new byte[1024];
			int len;
			while ((len = is.read(bytes)) != -1) {
				byte[] co = Arrays.copyOf(bytes, len);
				sb.append(new String(co, StandardCharsets.UTF_8));
			}
			return sb.toString();
		}
	}

}
