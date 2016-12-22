package yks;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import utils.DBUtil;

public class YKSTest {
	
	private static final String URL = "jdbc:mysql://127.0.0.1:3306/ebaytest?rewriteBatchedStatements=true";
	private static final String NAME = "root";
	private static final String PASSWORD = "root";
	
	@Test
	public void testQuery() {
		String sql = "select * from smt_admin limit 3";
		try {
			DBUtil db = new DBUtil();
			db.getResultData(sql);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testInsert() {
		String sql = "insert into erpdata (order_code,tracking_code) values (?,?);";
		try {
			List<Object> params = new ArrayList<>();
			params.add("testcode001");
			params.add("testcode001");
			DBUtil db = new DBUtil();
			Object obj = db.insertOrUpdate(sql, params);
			System.out.println(obj);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testUpdate() {
		String sql = "update erpdata set order_code = ? where id = ?";
		try {
			List<Object> params = new ArrayList<>();
			params.add("testcode002");
			params.add(2);
			DBUtil db = new DBUtil();
			db.insertOrUpdate(sql, params);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 有棵树作业
	 */
	@Test
	public void testInsertBatch() {
		String sql = "insert into erpdata (order_code,tracking_code) values (?,?)";
		String query = "select * from erpdata";
		try {
			DBUtil db = new DBUtil();
			List<Map<String, Object>> data = db.getResultData(query);
			List<List<Object>> list = new ArrayList<>();
			for(Map<String,Object> m : data){
				List<Object> row = new ArrayList<>();
				for(Map.Entry<String, Object> entry : m.entrySet()){
					if(!entry.getKey().equals("id")){
						row.add(entry.getValue());
					}
				}
				list.add(row);
			}
			new DBUtil(URL,NAME,PASSWORD,null).insertBatch(sql, list);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
