package util;

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

public class DBpool {

	private final String url;
	private final String user;
	private final String password;

	private static String driverClassName = "com.mysql.jdbc.Driver";

	private final int POOL_SIZE;
	private final RingBuffer pool;

	private SequenceNum block;// 用于判断阻塞等待的线程数量
	private volatile boolean memoryBarrier = true;// 提供内存屏障支持
	@SuppressWarnings("unused")
	private volatile boolean mb = true;// 提供内存屏障支持

	private static DBpool instance = new DBpool();

	private DBpool() {
		
		Map<String, String> conf = new XML().mysqlConf();
		this.url = conf.get("url");
		this.user = conf.get("user");
		this.password = conf.get("password");

		this.POOL_SIZE = 10;
		this.pool = new RingBuffer(POOL_SIZE);
		this.block = new SequenceNum();
		try {
			Class.forName(DBpool.driverClassName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} // 加载类到内存

		add_connection(this.POOL_SIZE - 1);
		System.out.println("dbpool start");
	}
	// ××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××
		// 连接池核心函数
		private void add_connection(int num) {// 向连接池pool添加连接
			for (int i = 0; i < num; ++i) {
				try {
					Connection conn = DriverManager.getConnection(this.url, this.user, this.password);
					pool.add_element(conn);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}

		private Connection get_conection() {// 相当于消费者
			Object conn = pool.get_element();
			if (conn != null) {// 连接池不为空
				return (Connection) conn;
			}

			this.block.increase();
			mb = memoryBarrier;// 在block变量之后添加内存屏障，该指令后面的指令不会被重排序到前面

			synchronized (pool) {
				while (pool.isEmpty()) {
					try {
						pool.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

				this.block.decrease();
				mb = memoryBarrier;// 在block变量之后添加内存屏障，该指令后面的指令不会被重排序到前面

				conn = pool.get_element();
				return (Connection) conn;
			}
		}

		private void release_conection(Connection conn) {// 相当于生产者
			pool.add_element(conn);

			memoryBarrier = true;// 内存屏障，保证之前的指令不会重排序到后面
			if (block.get() > 0) {// 存在阻塞的线程
				synchronized (pool) {
					pool.notifyAll();
				}
			}
		}

		public static DBpool get_instance() {
			return instance;
		}

		// ××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××××
		// 连接池外围函数
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

		public List<Map<String, Object>> executeQuery(String queryString) {// 查询
			Connection conn = null;
			Connection e_conn = null;// 错误状态下重新分配的connection
			Statement stmt = null;
			Statement e_stmt = null;// e_conn生成的statement
			ResultSet rs = null;
			ResultSet e_rs = null;// e_stmt返回的结果集
			boolean connection_timeout = false;
			List<Map<String, Object>> result = null;
			try {
				conn = get_conection();
				stmt = conn.createStatement();
				rs = stmt.executeQuery(queryString);// 出问题
				result = resultSet_to_obj(rs);
				return result;
			} catch (Exception e1) {// 可能连接失效
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
				} finally {// 释放资源
					try {
						e_rs.close();
						e_stmt.close();
						release_conection(e_conn);
					} catch (SQLException e3) {
						e3.printStackTrace();
					}
				}
			} finally {// 释放资源
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

		public boolean executeUpdate(String queryString) {// 更新
			Connection conn = null;
			Connection e_conn = null;// 错误状态下重新分配的connection
			Statement stmt = null;
			Statement e_stmt = null;// e_conn生成的statement
			boolean connection_timeout = false;
			try {
				conn = get_conection();
				stmt = conn.createStatement();
				stmt.executeUpdate(queryString);// 出问题
				return true;
			} catch (Exception e1) {// 可能连接失效
				try {
					connection_timeout = true;
					e_conn = DriverManager.getConnection(this.url, this.user, this.password);
					// 重新查询
					e_stmt = e_conn.createStatement();
					e_stmt.executeUpdate(queryString);
					return true;
				} catch (Exception e2) {
					e2.printStackTrace();
					return false;
				} finally {// 释放资源
					try {
						e_stmt.close();
						release_conection(e_conn);
					} catch (SQLException e3) {
						e3.printStackTrace();
					}
				}
			} finally {// 释放资源
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
}
