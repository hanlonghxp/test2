package utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DBUtil {

	private String url = "jdbc:mysql://127.0.0.1:3306/test?rewriteBatchedStatements=true";
	private String username = "root";
	private String password = "root";
	private String driver = "com.mysql.jdbc.Driver";

	private Connection conn = null;
	private PreparedStatement ps = null;
	private ResultSet rs = null;

	public DBUtil() {
	}

	public DBUtil(String url, String username, String password, String driver) {
		this.url = url;
		this.username = username;
		this.password = password;
		if (driver != null && driver.length() > 0)
			this.driver = driver;
	}

	public Connection getConnection() throws ClassNotFoundException, SQLException {
		Class.forName(driver);
		if (conn == null || conn.isClosed()) {
			conn = DriverManager.getConnection(url, username, password);
		}
		return conn;
	}

	public void destory() {
		try {
			if (rs != null) {
				rs.close();
			}
			if (ps != null) {
				ps.close();
			}
			if (conn != null) {
				conn.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public Object insertOrUpdate(String sql, List<Object> params) throws ClassNotFoundException, SQLException {
		try {
			Connection conn = this.getConnection();
			ps = conn.prepareStatement(sql);
			for (int i = 0; i < params.size(); i++) {
				ps.setObject(i + 1, params.get(i));
			}
			ps.executeUpdate();
			rs = ps.getGeneratedKeys();
			Object obj = null;
			while (rs.next()) {
				obj = rs.getObject(1);
				break;
			}
			return obj;
		} finally {
			this.destory();
		}
	}

	public void insertBatch(String sql, List<List<Object>> data) throws SQLException, ClassNotFoundException {
		try {
			Connection conn = this.getConnection();
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(sql);
			for (int i = 0; i < data.size(); i++) {
				for (int j = 0; j < data.get(i).size(); j++) {
					ps.setObject(j + 1, data.get(i).get(j));
				}
				ps.addBatch();
			}
			ps.executeBatch();
			conn.commit();
		} finally {
			this.destory();
		}
	}

	public List<Map<String, Object>> getResultData(String sql) throws ClassNotFoundException, SQLException {
		try {
			Connection conn = this.getConnection();
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			return this.result(rs);
		} finally {
			this.destory();
		}
	}

	private List<Map<String, Object>> result(ResultSet rs) throws SQLException {
		List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
		ResultSetMetaData rsm = rs.getMetaData();
		while (rs.next()) {
			Map<String, Object> row = new HashMap<>();
			int columnCount = rsm.getColumnCount();
			for (int i = 1; i <= columnCount; i++) {
				row.put(rsm.getColumnName(i), rs.getObject(i));
			}
			data.add(row);
		}
		return data;
	}
}
