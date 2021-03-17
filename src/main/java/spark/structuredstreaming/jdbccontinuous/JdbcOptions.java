package main.java.spark.structuredstreaming.jdbccontinuous;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

/**
 * @author caik
 * @since 2021/3/11
 */
public class JdbcOptions implements Serializable {

	Properties properties = new Properties();

	public JdbcOptions(Map<String, String> map) {
		properties.putAll(map);
	}

	public JdbcOptions(String driver, String url, String user, String pwd, String table, String timestamp) {
		properties.put("driver", driver);
		properties.put("url", url);
		properties.put("user", user);
		properties.put("pwd", pwd);
		properties.put("table", table);
		properties.put("timestamp", timestamp);
	}

	public String getDriver() {
		return properties.getProperty("driver");
	}

	public String getUrl() {
		return properties.getProperty("url");
	}

	public String getUser() {
		return properties.getProperty("user");
	}

	public String getPwd() {
		return properties.getProperty("pwd");
	}

	public String getTable() {
		return properties.getProperty("table");
	}

	public String getTimestamp() {
		return properties.getProperty("timestamp");
	}

	public Properties getProperties(){return properties;}

}
