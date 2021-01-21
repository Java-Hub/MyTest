package main.java.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import oracle.jdbc.driver.OracleDriver;

public class TestClose {

	public static void main(String[] args) throws ClassNotFoundException, SQLException {

		Class.forName(OracleDriver.class.getName());
		
		Connection cn = DriverManager.getConnection("jdbc:oracle:thin:172.21.11.12:1521/orcl", "root", "root");
		Statement st = cn.createStatement();
		ResultSet rs = st.executeQuery("select * from datatype");
		System.out.println("rs:"+rs.isClosed());
		System.out.println("st:"+st.isClosed());
		System.out.println("rs:"+cn.isClosed());
		cn.close();
		System.out.println("rs:"+rs.isClosed());
		System.out.println("st:"+st.isClosed());
		System.out.println("rs:"+cn.isClosed());
	}

}
