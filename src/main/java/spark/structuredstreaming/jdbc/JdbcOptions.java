package main.java.spark.structuredstreaming.jdbc;

import scala.collection.immutable.Map;

import java.io.Serializable;

/**
 * jdbcSource参数
 * @author caik
 * @since 2021/3/3
 */
public class JdbcOptions implements Serializable {

	private static final long serialVersionUID = 9145112216274336746L;

	public static final String DRIVER = "driver";

	public static final String URL = "url";

	public static final String TABLE = "table";

	public static final String USER = "user";

	public static final String PWD = "password";

	public static final String TIMESTAMP = "timestamp";

	private Map<String,String> map;

	public JdbcOptions(Map<String,String> map) {
		this.map = map;
	}

	public String getDriver(){
		return map.apply(DRIVER);
	}

	public String getUrl(){
		return map.apply(URL);
	}

	public String getTable(){
		return map.apply(TABLE);
	}

	public String getUser(){
		return map.apply(USER);
	}

	public String getPwd(){
		return map.apply(PWD);
	}

	public String getTimestamp(){
		return map.apply(TIMESTAMP);
	}

}
