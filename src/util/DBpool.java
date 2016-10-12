package util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Vector;

public class DBpool {

	private static String url = "";
	private static String user = "";
	private static String password = "";

	private static String driverClassName = "com.mysql.jdbc.Driver";

	private int commonPoolSize = 10;
	private int maxPoolSize = 15;
	private int nowTotalConnections = 10;

	private static DBpool instance = new DBpool("log0");

	private Queue<Connection> pool = new LinkedList<Connection>();
	private Vector<Connection> connectionsInUse = new Vector<Connection>(commonPoolSize);

	private DBpool(String dbName) {
		DBpool.url = "jdbc:mysql://localhost:3306/"+dbName;
		DBpool.user = "root";
		DBpool.password = "";
		try {
			Class.forName(DBpool.driverClassName);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		addConnection(commonPoolSize);
		System.out.println("Á¬½Ó³ØÆô¶¯");
	}

	public static DBpool getInstance() {
		return instance;
	}

	public String get_url() {
		return url;
	}
	

	public String get_user() {
		return user;
	}
	
	public String get_password() {
		return password;
	}
	
	public String get_driverClassName() {
		return driverClassName;
	}

	private List<Map<String, Object>> resultSet_to_obj(ResultSet r) {
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
		try {
			ResultSetMetaData rsmd = r.getMetaData();
			int numberOfColumns = rsmd.getColumnCount();
			while (r.next()) {
				Map<String, Object> row = new HashMap<String, Object>();
				for (int i = 1; i <= numberOfColumns; ++i) {
					String name = rsmd.getColumnName(i);
					Object value = r.getObject(name);
					row.put(name, value);
				}
				result.add(row);
			}
		} catch (Exception e) {

			e.printStackTrace();
		}
		return result;
	}

	private Connection newConnection() {
		try {
			Connection conn = DriverManager.getConnection(DBpool.url, DBpool.user, DBpool.password);
			return conn;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	private void addConnection(int num) {
		synchronized (pool) {
			for (int i = 0; i < num; ++i) {
				try {
					Connection conn = newConnection();
					if (conn != null) {
						pool.offer(conn);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	public Connection getConnection() {
		synchronized (pool) {
			while (pool.isEmpty() && (nowTotalConnections == maxPoolSize)) {
				try {
					pool.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			if (pool.isEmpty() && (nowTotalConnections < maxPoolSize)) {
				Connection conn = newConnection();
				connectionsInUse.add(conn);
				++nowTotalConnections;
				return conn;
			}
			if (!pool.isEmpty()) {
				Connection conn = pool.poll();
				connectionsInUse.add(conn);
				return conn;
			}
			return null;
		}
	}

	public void releaseConnection(Connection cn) {
		if (connectionsInUse.remove(cn)) {
			if (connectionsInUse.size() < commonPoolSize / 2 && nowTotalConnections > commonPoolSize) {
				try {
					cn.close();
					--nowTotalConnections;
				} catch (Exception e) {
					e.printStackTrace();
				}
			} else {
				synchronized(pool){
					pool.offer(cn);
					pool.notifyAll();
				}
			}
		}
	}

	public List<Map<String, Object>> executeQuery(String queryString) {
		Connection conn = null;
		Connection e_conn = null;
		Statement stmt = null;
		Statement e_stmt = null;
		ResultSet rs = null;
		ResultSet e_rs = null;
		boolean connection_timeout = false;
		List<Map<String, Object>> result = null;
		try {
			conn = getConnection();
			stmt = conn.createStatement();
			rs = stmt.executeQuery(queryString);
			result = resultSet_to_obj(rs);
			return result;
		} catch (Exception e1) {
			try {

				connectionsInUse.remove(conn);
				connection_timeout = true;
				e_conn = newConnection();
				connectionsInUse.add(e_conn);

				e_stmt = e_conn.createStatement();
				e_rs = e_stmt.executeQuery(queryString);
				result = resultSet_to_obj(e_rs);
				return result;
			} catch (Exception e2) {
				return null;
			} finally {
				try {
					e_rs.close();
					e_stmt.close();
					releaseConnection(e_conn);
				} catch (SQLException e3) {
					e3.printStackTrace();
				}
			}
		} finally {
			try {
				if( !connection_timeout ){
					rs.close();
					stmt.close();
					releaseConnection(conn);
				} else{
					stmt.close();
					conn.close();
				}
			} catch (SQLException e4) {
				e4.printStackTrace();
			}
		}
	}

	public boolean executeUpdate(String queryString) {
		Connection conn = null;
		Connection e_conn = null;
		Statement stmt = null;
		Statement e_stmt = null;
		boolean connection_timeout = false;
		try {
			conn = getConnection();
			stmt = conn.createStatement();
			stmt.executeUpdate(queryString);
			return true;
		} catch (Exception e1) {
			try {
				connection_timeout = true;
				connectionsInUse.remove(conn);
				e_conn = newConnection();
				connectionsInUse.add(e_conn);

				e_stmt = e_conn.createStatement();
				e_stmt.executeUpdate(queryString);
				return true;
			} catch (Exception e2) {
				return false;
			} finally {
				try {
					e_stmt.close();
					releaseConnection(e_conn);
				} catch (SQLException e3) {
					e3.printStackTrace();
				}
			}
		} finally {
			try {
				if(!connection_timeout){
					stmt.close();
					releaseConnection(conn);
				} else{
					stmt.close();
					conn.close();
				}
			} catch (SQLException e4) {
				e4.printStackTrace();
			}
		}
	}

	public synchronized void closePool() {

	}
}
