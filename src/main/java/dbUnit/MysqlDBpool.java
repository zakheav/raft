package dbUnit;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import util.XML;

public class MysqlDBpool implements DBpool {

	private final String url;
	private final String user;
	private final String password;

	private static String driverClassName = "com.mysql.jdbc.Driver";

	private final int POOL_SIZE;
	private final RingBuffer pool;

	private SequenceNum block;
	private volatile boolean memoryBarrier = true;
	@SuppressWarnings("unused")
	private volatile boolean mb = true;

	public MysqlDBpool() {

		Map<String, String> conf = new XML().mysqlConf();
		this.url = conf.get("url");
		this.user = conf.get("user");
		this.password = conf.get("password");

		this.POOL_SIZE = 10;
		this.pool = new RingBuffer(POOL_SIZE);
		this.block = new SequenceNum();
		try {
			Class.forName(MysqlDBpool.driverClassName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		add_connection(this.POOL_SIZE - 1);
		System.out.println("dbpool start");
	}

	// ××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××
	// core function
	private void add_connection(int num) {
		for (int i = 0; i < num; ++i) {
			try {
				Connection conn = DriverManager.getConnection(this.url, this.user, this.password);
				pool.add_element(conn);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	private Connection get_conection() {
		Object conn = pool.get_element();
		if (conn != null) {
			return (Connection) conn;
		}

		this.block.increase();
		mb = memoryBarrier;

		synchronized (pool) {
			while (pool.isEmpty()) {
				try {
					pool.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			this.block.decrease();
			mb = memoryBarrier;

			conn = pool.get_element();
			return (Connection) conn;
		}
	}

	private void release_conection(Connection conn) {
		pool.add_element(conn);

		memoryBarrier = true;
		if (block.get() > 0) {
			synchronized (pool) {
				pool.notifyAll();
			}
		}
	}

	// ××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××
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
			conn = get_conection();
			stmt = conn.createStatement();
			rs = stmt.executeQuery(queryString);
			result = resultSet_to_obj(rs);
			return result;
		} catch (Exception e1) {
			try {
				connection_timeout = true;
				e_conn = DriverManager.getConnection(this.url, this.user, this.password);
				// 重新查询
				e_stmt = e_conn.createStatement();
				e_rs = e_stmt.executeQuery(queryString);
				result = resultSet_to_obj(e_rs);
				return result;
			} catch (Exception e2) {
				e2.printStackTrace();
				return null;
			} finally {
				try {
					e_rs.close();
					e_stmt.close();
					release_conection(e_conn);
				} catch (SQLException e3) {
					e3.printStackTrace();
				}
			}
		} finally {
			try {
				if (!connection_timeout) {
					rs.close();
					stmt.close();
					release_conection(conn);
				} else {
					stmt.close();
					conn.close();
				}
			} catch (SQLException e4) {
				e4.printStackTrace();
			}
		}
	}

	public boolean executeUpdate(List<String> queryStringList) {
		Connection conn = null;
		Connection e_conn = null;
		Statement stmt = null;
		Statement e_stmt = null;
		boolean connection_timeout = false;
		try {
			conn = get_conection();
			conn.setAutoCommit(false);
			stmt = conn.createStatement();
			for (String queryString : queryStringList)
				stmt.executeUpdate(queryString);
			conn.commit();
			return true;
		} catch (Exception e1) {
			try {
				conn.rollback();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			try {
				connection_timeout = true;
				e_conn = DriverManager.getConnection(this.url, this.user, this.password);
				e_conn.setAutoCommit(false);
				
				e_stmt = e_conn.createStatement();
				for (String queryString : queryStringList) {
					e_stmt.executeUpdate(queryString);
				}
				conn.commit();
				return true;
			} catch (Exception e2) {
				try {
					e_conn.rollback();
				} catch (SQLException e) {
					e.printStackTrace();
				}
				e2.printStackTrace();
				return false;
			} finally {
				try {
					e_stmt.close();
					release_conection(e_conn);
				} catch (SQLException e3) {
					e3.printStackTrace();
				}
			}
		} finally {
			try {
				if (!connection_timeout) {
					stmt.close();
					release_conection(conn);
				} else {
					stmt.close();
					conn.close();
				}
			} catch (SQLException e4) {
				e4.printStackTrace();
			}
		}
	}

	
	// ××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××
	// raft log operation
	public Integer get_lastLogIndex() {
		String queryString = "select max(logIndex) as lastLogIndex from log";

		Object o = executeQuery(queryString).get(0).get("lastLogIndex");
		Integer lastLogIndex = o == null ? 0 : (Integer) o;
		return lastLogIndex;
	}

	public Integer get_lastLogTerm(Integer lastLogIndex) {
		String queryString = "select term from log where logIndex = " + lastLogIndex;
		List<Map<String, Object>> result = executeQuery(queryString);
		return result.isEmpty() ? 1 : (Integer) result.get(0).get("term");
	}

	public List<Map<String, Object>> get_recentSubmitCommandId(Integer temp) {
		String queryString = "select logIndex, commandId from log where logIndex > " + temp + " order by logIndex asc";
		return executeQuery(queryString);
	}

	public List<Map<String, Object>> get_logByIndex(Integer index) {
		String queryString = "select * from log where logIndex = " + index;
		return executeQuery(queryString);
	}

	public void build_log() {
		String queryString = "CREATE TABLE IF NOT EXISTS `log` ( `logIndex` int(11) DEFAULT NULL, `term` int(11) DEFAULT NULL, `command` varchar(1024) DEFAULT NULL, `commandId` varchar(64) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8";
		List<String> queryStringList = new ArrayList<String>();
		queryStringList.add(queryString);
		executeUpdate(queryStringList);
	}

	public void commit_command(Integer logIndex, Integer term, String command, String commandId) {
		String queryString = "insert into log(logIndex, term, command, commandId) values(" + logIndex + "," + term
				+ ",'" + command + "','" + commandId + "')";
		List<String> queryStringList = new ArrayList<String>();
		queryStringList.add(queryString);
		queryStringList.add(command);
		executeUpdate(queryStringList);// commit
	}
}
