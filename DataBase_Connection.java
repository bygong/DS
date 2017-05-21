import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

public class DataBase_Connection {
	//JDBC driver name & database url
	static final String Driver = "com.mysql.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://mpcs53001.cs.uchicago.edu/xhyDB";
	
	//DB credentials
	static final String user = "xhy";
	static final String password = "maipohfa";
	
	//database table names
	static final String Continent = "continent_exchange";	//continent look-up table
	private String Stock_name;								//exchange look-up table
	private String Stock_price;								//stock price look-up table
	private String Stock_qty;								//stock qty look-up table
	private String Stock_qty_log;							//log qty of trading
	private String tmp_qty;									//temporary table to save qty
//	private int stock_id = -1;								//stock id in exchange_name table
	
	
	public DataBase_Connection(String exchange_name) {
		Stock_name = exchange_name + "_name";
		Stock_price = exchange_name + "_price";
		Stock_qty = exchange_name + "_qty";
		Stock_qty_log = Stock_qty + "_log";
		tmp_qty = Stock_qty + "_record";
	}
	
//	public int getStockId() {
//		return stock_id;
//	}
	
	//Return continent where the exchange is located in
	//If continent is not found, return null
	public String QueryContinet(String exchange) {
		String continent = null;
		try {
			//Register JDBC driver
			Class.forName(Driver);
			
			//Open a connection
			Connection conn = DriverManager.getConnection(DB_URL, user, password);
			
			//Query
			Statement stmt = conn.createStatement();
			String sql;
			sql = "SELECT continent FROM " + Continent + " WHERE exchange=\'" + exchange + "\'";
			ResultSet rs = stmt.executeQuery(sql);
			
			//Extract data
			while (rs.next()) {
				//Retrieve by column name
				continent = rs.getString("continent");
				
				//Display
				System.out.println("Exchange is in " + continent);
			}
			
			//Clean-up
			rs.close();
			stmt.close();
			conn.close();
		}catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
		
		return continent;
	}
	
	//If the stock is in the exchange, return stock_id
	//If exchange is not found, return -1
	public int QueryStockID(String stock) {
		int stock_id = -1;
		try {
			//Register JDBC driver
			Class.forName(Driver);
			
			//Open a connection
			Connection conn = DriverManager.getConnection(DB_URL, user, password);
			
			//Query
			Statement stmt = conn.createStatement();
			String sql;
			sql = "SELECT * FROM " + Stock_name + " WHERE stock_name=\'" + stock + "\'";
			ResultSet rs = stmt.executeQuery(sql);
			
			//Extract data
			while (rs.next()) {
				//Retrieve by column name
				stock_id = rs.getInt("id");
				
				//Display
				System.out.println("id = " + stock_id);
			}
			
			//Clean-up
			rs.close();
			stmt.close();
			conn.close();
		}catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
		
		return stock_id;
	}
	
	//Return the stock price at timer = t with stock_id
	//If price is not found, return default value -1
	public float QueryPrice(int t, int stock_id) {
		float price = -1;
		try {
			//Register JDBC driver
			Class.forName(Driver);
			
			//Open a connection
			Connection conn = DriverManager.getConnection(DB_URL, user, password);
			
			//Query
			Statement stmt = conn.createStatement();
			String sql;
			sql = "SELECT * FROM " + Stock_price + " WHERE timer=" + t;
			ResultSet rs = stmt.executeQuery(sql);
			
			//Extract data
			while (rs.next()) {
				//Retrieve by column index
				int index = stock_id + 1;			//timer is the first element
				price = rs.getFloat(index);
				
				//Display
				System.out.println("Price at timer = " + t + "  is " + price);
			}
			
			//Clean-up
			rs.close();
			stmt.close();
			conn.close();
		}catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
		
		return price;
	}
	
	//Return the stock quantity at timer = t with stock_id
	//If quantity is not found, return default value -1
	public int QueryQty(int t, int stock_id) {
		int qty = -1;
		try {
			//Register JDBC driver
			Class.forName(Driver);
			
			//Open a connection
			Connection conn = DriverManager.getConnection(DB_URL, user, password);
			
			//Query
			Statement stmt = conn.createStatement();
			String sql;
			sql = "SELECT * FROM " + Stock_qty + " WHERE timer=" + t;
			ResultSet rs = stmt.executeQuery(sql);
			
			//Extract data
			while (rs.next()) {
				//Retrieve by column index
				int index = stock_id + 1;			//timer is the first element
				qty = rs.getInt(index);
				
				//Display
				System.out.println("Quantity at timer = " + t + "  is " + qty);
			}
			
			//Clean-up
			rs.close();
			stmt.close();
			conn.close();
		}catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
		
		return qty;
	}
	
	//Return stock prices at timer = t
	public ArrayList<Float> QueryPriceAll(int t) {
		ArrayList<Float> tmp = new ArrayList<>();
		int index = 2;
		try {
			//Register JDBC driver
			Class.forName(Driver);
			
			//Open a connection
			Connection conn = DriverManager.getConnection(DB_URL, user, password);
			
			//Query
			Statement stmt = conn.createStatement();
			String sql;
			sql = "SELECT * FROM " + Stock_price + " WHERE timer=" + t;
			ResultSet rs = stmt.executeQuery(sql);
			
			//Extract data
			System.out.println("Extract data");
			float price = 0;
			while (rs.next()) {
				//Retrieve by column index
				while(index > 0) {
					try {
						price = rs.getFloat(index);
					}catch (Exception e) {
						System.err.println("BREAK");
						break;
					}
					tmp.add(price);
					index++;
					
					//Display
					System.out.println(price);
				}
			}
			System.err.println(index);
			System.err.println(tmp.size());
			//Clean-up
			rs.close();
			stmt.close();
			conn.close();
		}catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
		
		return tmp;
	}
	
	//Return stock quantities at timer = t
	public ArrayList<Integer> QueryQuantityAll(int t) {
		ArrayList<Integer> tmp = new ArrayList<>();
		int index = 2;
		try {
			//Register JDBC driver
			Class.forName(Driver);
			
			//Open a connection
			Connection conn = DriverManager.getConnection(DB_URL, user, password);
			
			//Query
			Statement stmt = conn.createStatement();
			String sql;
			sql = "SELECT * FROM " + Stock_qty + " WHERE timer=" + t;
			ResultSet rs = stmt.executeQuery(sql);
			
			//Extract data
			int qty = 0;
			while (rs.next()) {
				//Retrieve by column index
				while(index > 0) {
					try {
						qty = rs.getInt(index);
					}catch (Exception e) {
						System.err.println("BREAK");
						break;
					}
					tmp.add(qty);
					index++;
					
					//Display
					System.out.println(qty);
				}
			}
			System.err.println(index);
			System.err.println(tmp.size());
			
			//Clean-up
			rs.close();
			stmt.close();
			conn.close();
		}catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
		
		return tmp;
	}
	
	//Update quantity at timer = t with stock_id
//	public boolean updateQty(int stock_id, int qty, int t) {
//		try {
//			//Register JDBC driver
//			Class.forName(Driver);
//			
//			//Open a connection
//			Connection conn = DriverManager.getConnection(DB_URL, user, password);
//			
//			//Update
//			Statement stmt = conn.createStatement();
//			String sql;
//			sql = "UPDATE " + Stock_qty + " SET " + "C" + stock_id + "=" + qty + " WHERE timer = " + t;
//			System.out.println(sql);
//			stmt.execute(sql);
//			
//			//Clean-up
//			stmt.close();
//			conn.close();
//		}catch (Exception e) {
//			System.err.println(e.getMessage());
//			e.printStackTrace();
//			return false;
//		}
//		
//		return true;
//	}
	
	public boolean updateQty(int stock_id, ArrayList<Integer> tmp, int t, boolean all) {
		int size = tmp.size();
		try {
			//Register JDBC driver
			Class.forName(Driver);
			
			//Open a connection
			Connection conn = DriverManager.getConnection(DB_URL, user, password);
			
			//Update
			Statement stmt = conn.createStatement();
			String sql;
			//update all
			if (all) {
				for (int i=0; i<size; i++) {
					//stock_id starts from 1
					int id = i +1;		
					sql = "UPDATE " + Stock_qty_log + " SET " + "C" + id + "=" + tmp.get(i) + " WHERE timer = " + t;
					System.out.println(sql);
					stmt.execute(sql);
				}
			}
			
			//update only one stock
			else {
				sql = "UPDATE " + tmp_qty + " SET " + "C" + stock_id + "=" + tmp.get(stock_id -1);
				System.out.println(sql);
				stmt.execute(sql);
			}
			
			//Clean-up
			stmt.close();
			conn.close();
		}catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	//update to tmp_qty
	//if update whole tmp_qty, all = true
	//if update only single stock, all = false
	public boolean to_tmpQty(int stock_id, ArrayList<Integer> tmp, boolean all) {
		int size = tmp.size();
		try {
			//Register JDBC driver
			Class.forName(Driver);
			
			//Open a connection
			Connection conn = DriverManager.getConnection(DB_URL, user, password);
			
			//Update
			Statement stmt = conn.createStatement();
			String sql;
			//update all
			if (all) {
				for (int i=0; i<size; i++) {
					//stock_id starts from 1
					int id = i +1;		
					sql = "UPDATE " + tmp_qty + " SET " + "C" + id + "=" + tmp.get(i);
					System.out.println(sql);
					stmt.execute(sql);
				}
			}
			
			//update only one stock
			else {
				sql = "UPDATE " + tmp_qty + " SET " + "C" + stock_id + "=" + tmp.get(stock_id -1);
				System.out.println(sql);
				stmt.execute(sql);
			}
			
			//Clean-up
			stmt.close();
			conn.close();
		}catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	//extract from tmp_qty
	//used when first time launch or after exchange recovers from crash
	public ArrayList<Integer> from_tmpQty() {
		ArrayList<Integer> tmp = new ArrayList<>();
		int index = 2;
		try {
			//Register JDBC driver
			Class.forName(Driver);
			
			//Open a connection
			Connection conn = DriverManager.getConnection(DB_URL, user, password);
			
			//Query
			Statement stmt = conn.createStatement();
			String sql;
			sql = "SELECT * FROM " + tmp_qty;
			ResultSet rs = stmt.executeQuery(sql);
			
			//Extract data
			int qty = 0;
			while (rs.next()) {
				//Retrieve by column index
				while(index > 0) {
					try {
						qty = rs.getInt(index);
					}catch (Exception e) {
						System.err.println("BREAK");
						break;
					}
					tmp.add(qty);
					index++;
					
					//Display
					System.out.println(qty);
				}
			}
			System.err.println(index);
			System.err.println(tmp.size());
			
			//Clean-up
			rs.close();
			stmt.close();
			conn.close();
		}catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
		
		return tmp;
	}
}
