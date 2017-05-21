import java.awt.List;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.channels.*;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Time;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;

import org.jgroups.JChannel;

public class Exchange{

	public static Address address;
	static ExchangeClient client;
	static ExchangeServer server;
	static Superpeer superpeer;
	static boolean clientInitiated = false, registered = false, systemInitiated = false;
	static Integer exchangeTime = 0;
	
	public DataBase_Connection my_db;			//database of the exchange
	private ArrayList<Float> price;				//price of every stock in the exchange per timer(from table)
	private ArrayList<Integer> table_quantity;	//quantity of every stock in the exchange per timer(from table)
	private ArrayList<Integer> write_quantity;	//quantity written to tmp_quantity table
	static Timer timer;
	
	ExchangeTimer timerTask = new ExchangeTimer();
	
	public static final int timeout = 5000, timeInterval = 3000;
	
	static Address housekeeperAddress = new Address("Housekeeper", null, "localhost", 8080);
	static Address superPeerAddress;
	
	//address cache pool
	protected static HashMap<String, Address> addressPool = new HashMap<String, Address>();
	protected static HashMap<String, Integer> stockShelf;
	protected static HashMap<String, DNSEntry> dnsTable = new HashMap<>();
	protected static HashMap<String, Address> burnedInExchangeAddresses = new HashMap<String, Address>(){
		{
			put("New York Stock Exchange", new Address("New York Stock Exchange","America","localhost",10001));
			put("Euronext Paris", new Address("Euronext Paris","Europe","localhost",10003));
			put("Frankfurt", new Address("Frankfurt","Europe","localhost",10005));
			put("London", new Address("London","Europe","localhost",10007));
			put("Tokyo", new Address("Tokyo","Asia","localhost",10009));
			put("Hong Kong", new Address("Hong Kong","Asia","localhost",10011));
			put("Shanghai", new Address("Shanghai","Asia","localhost",10013));
			put("Brussels", new Address("Brussels","Europe","localhost",10015));
			put("Lisbon", new Address("Lisbon","Europe","localhost",10017));
			put("Shenzhen", new Address("Shenzhen","Asia","localhost",10019));
			put("Toronto", new Address("Toronto","America","localhost",10021));
			put("Bombay", new Address("Bombay","Asia","localhost",10023));
			put("Zurich", new Address("Zurich","Europe","localhost",10025));
			put("Sydney", new Address("Sydney","Asia","localhost",10027));
			put("Seoul", new Address("Seoul","Asia","localhost",10029));
			put("Johannesburg", new Address("Johannesburg","Europe","localhost",10031));
			put("Paulo,Sao", new Address("Paulo,Sao","America","localhost",10033));
		}
	};
	
	protected static HashMap<String, Address> burnedInSuperpeerAddresses = new HashMap<String, Address>(){
		{
			put("New York Stock Exchange", new Address("New York Stock Exchange","America","localhost",10002));
			put("Euronext Paris", new Address("Euronext Paris","Europe","localhost",10004));
			put("Frankfurt", new Address("Frankfurt","Europe","localhost",10006));
			put("London", new Address("London","Europe","localhost",10008));
			put("Tokyo", new Address("Tokyo","Asia","localhost",10010));
			put("Hong Kong", new Address("Hong Kong","Asia","localhost",10012));
			put("Shanghai", new Address("Shanghai","Asia","localhost",10014));
			put("Brussels", new Address("Brussels","Europe","localhost",10016));
			put("Lisbon", new Address("Lisbon","Europe","localhost",10018));
			put("Shenzhen", new Address("Shenzhen","Asia","localhost",10020));
			put("Toronto", new Address("Toronto","America","localhost",10022));
			put("Bombay", new Address("Bombay","Asia","localhost",10024));
			put("Zurich", new Address("Zurich","Europe","localhost",10026));
			put("Sydney", new Address("Sydney","Asia","localhost",10028));
			put("Seoul", new Address("Seoul","Asia","localhost",10030));
			put("Johannesburg", new Address("Johannesburg","Europe","localhost",10032));
			put("Paulo,Sao", new Address("Paulo,Sao","America","localhost",10034));
		}
	};
	
	protected static HashMap<String, String> initialSuperpeer = new HashMap<String, String>(){
		{
			put("America","New York Stock Exchange");
			put("Europe","London");
			put("Asia","Tokyo");
		}
	};
	
	protected static HashMap<String, MutualFund> mutualFundList = new HashMap<String, MutualFund>(){
		{
			put("Mutual_Fund_Banking_1", new MutualFund("Mutual_Fund_Banking_1", new HashMap<String,Double>(){
				{
					put("Deutsche Bank", 0.2);
					put("CREDEIT AGRICOLE", 0.2);
					put("SOCIETE GENERALE", 0.1);
					put("American Express", 0.2);
					put("Goldman Sachs", 0.2);
					put("JPMorgan Chase", 0.2);
					put("Nomura Holdings, Inc.", 0.2);
				}
			}));
			
			put("Mutual_Fund_Energy_1", new MutualFund("Mutual_Fund_Banking_1", new HashMap<String,Double>(){
				{
					put("Petrobras", 0.15);
					put("BP PLC", 0.15);
					put("TOTAL", 0.1);
					put("ExxonMobil", 0.2);
				}
			}));
			
			put("Mutual_Fund_Diversified_1", new MutualFund("Mutual_Fund_Banking_1", new HashMap<String,Double>(){
				{
					put("Swire Pacific Limited", 0.15);
					put("Softbank Corp.", 0.35);
					put("Sky PLC", 0.4);
					put("Deutsche Lufthansa", 0.1);
				}
			}));
		}
	};
	
	
	class ExchangeTimer extends TimerTask{
		@Override
		public void run() {
			for (String entry : dnsTable.keySet()){
				if ((dnsTable.get(entry).TTL--) == 0)
					dnsTable.remove(entry);
			}
			timer_tick();
		}
	}
	
	
	
	public static void main(String[] args) {
		//------------------validate input---------------
				if (args.length != 1)
				{
					System.out.println("Please input exactly 1 arguments.");
					return;
				}
				
				if (!burnedInExchangeAddresses.containsKey(args[0]))
				{
					System.out.println("Exchange doesn't exist!");
					return;
				}
				
		//-------------------------------------------------
		
		//start the exchange
		Exchange exchange = new Exchange(args[0], initialSuperpeer.get(burnedInExchangeAddresses.get(args[0]).continent) == args[0]);
		exchange.start();
	}
	
	//constructor, specifying its super peer
	public Exchange(String name, boolean isSuper) {
		my_db = new DataBase_Connection(name);
		
		//initialize write_quantity
		QueryQty();
		for (int i=0; i<price.size(); i++)
			write_quantity.add(table_quantity.get(i));
		//update to tmp_quantity table
		my_db.to_tmpQty(-1, write_quantity, true);
		if (isSuper)
		{
			becomeSuperpeer();
			registered = true;
		}
		superpeer.innerExchanges.put(address.name, address);
		
	}
	
	public void start(){
		try (
				BufferedReader userIn = new BufferedReader(new InputStreamReader(System.in));
				){
			
			//create server and client side of this peer
			//server runs as thread while main thread listening on user input
			server = new ExchangeServer(this);
			client = new ExchangeClient(this);
			
			server.clientDelegate = client;
			client.serverDelegate = server;
			
			
			
			while(!registered)
			{
				registered = client.sendHousekeeperRegister();
			}
			
			registered = false;
			
			while(!registered){
				registered = client.sendRegister();
			}

			server.start();
			

		}catch (Exception e) {
			System.out.println(e.toString());
		}
	}
	
	public boolean hasStock(String stockName) {
		return stockShelf.containsKey(stockName);
	}
	
	
	
	//add a dns entry to my pool
	public void addAddress(String s, Address a){
		addressPool.put(s, a);
	}
	
	
	Address routing(String stockName){
		Address dest;
		
		if (dnsTable.containsKey(stockName))
			return dnsTable.get(stockName).address;
		if (superpeer!=null){
			dest = superpeer.routeTo(stockName);
		}
		else{
			dest = client.sendRoute(superPeerAddress,stockName);
		}
		if (dest != null){
			addDNSEntry(stockName, dest);
		}
		return dest;
	};
	
	void addDNSEntry(String stockName, Address address){
		synchronized (dnsTable) {
			dnsTable.put(stockName, new DNSEntry(address));
		}
	}
	
	void removeDNSEntry(String stockName){
		synchronized (dnsTable) {
			dnsTable.put(stockName, new DNSEntry(address));
		}
	}

	
	boolean isSuperpeer(){
		return superpeer != null;
	}
	
	void becomeSuperpeer(){
		superpeer = new Superpeer(burnedInSuperpeerAddresses.get(address.name));
		superPeerAddress = null;
	}
	
	//Query all prices at exchangeTime = t
		public void QueryPrice() {
			price = my_db.QueryPriceAll(exchangeTime);
		}
		
		//Query all quantities at exchangeTime = t 
		public void QueryQty() {
			table_quantity = my_db.QueryQuantityAll(exchangeTime);
		}
		
//		public void updateQty(int arr_index, int qty) {
//			int stock_id = arr_index + 1;
//			my_db.updateQty(stock_id, qty, exchangeTime);
//		}
		
		//buy in local exchange
		//if succeed, return buy price; otherwise, return -1
		public double buy(int stock_id, int share) {
			int index = stock_id - 1;							//may be changed
			int qty_before = write_quantity.get(index);
			//no enough shares
			if (qty_before < share)
				return -1;
			int qty_after = qty_before - share;
			write_quantity.set(index, qty_after);
			//update to tmp_quantity table
			my_db.to_tmpQty(stock_id, write_quantity, false);
			return price.get(index);
		}
		
		//buy mutual fund
		//if succeed, return buy price; otherwise, return -1
		public double buyMutualFund(String stockName, int shares){
			MutualFund fund = mutualFundList.get(stockName);
			HashMap<String, Double> tmp = new HashMap<>();
			for (String stock : fund.proportion.keySet()){
				int id = my_db.QueryStockID((stock));
				double price;
				if (id != -1){
					if ((price = buy(id,shares)) > 0){
						tmp.put(stock,price);
					}else{
						break;
					}
				}
				else{
					Address dest = routing(stock);
					// not found
					if (dest == null)
						break;
					else{
						price = client.sendRemoteBuy(dest,stockName,shares);
						if (price < 0)
							break;
						tmp.put(stock,price);
					}
				}
			}
			
			if (tmp.size() < fund.proportion.size()){
				return -1;
			}else{
				double price = 0;
				for (String stock : fund.proportion.keySet()){
					price += tmp.get(stock) * fund.proportion.get(stock);
				}
				return price;
			}
			
		}
		
		
		public double sellMutualFund(String stockName, int shares){
			MutualFund fund = mutualFundList.get(stockName);
			HashMap<String, Double> tmp = new HashMap<>();
			for (String stock : fund.proportion.keySet()){
				int id = my_db.QueryStockID((stock));
				double price;
				if (id != -1){
					if ((price = sell(true, id,shares)) > 0){
						tmp.put(stock,price);
					}else{
						break;
					}
				}
				else{
					Address dest = routing(stock);
					// not found
					if (dest == null)
						break;
					else{
						price = client.sendRemoteSell(dest,stockName,shares);
						if (price < 0)
							break;
						tmp.put(stock,price);
					}
				}
			}
			
			if (tmp.size() < fund.proportion.size()){
				return -1;
			}else{
				double price = 0;
				for (String stock : fund.proportion.keySet()){
					price += tmp.get(stock) * fund.proportion.get(stock);
				}
				return price;
			}
			
		}
		//check if the user has so many shares of the stock
		public boolean enough_share(String user, int share) {
			//trader's share -> need to be changed further
			int trader_share = 0;
			
			if (trader_share < share)
				return false;
			return true;
		}
		
		//sell in local exchange
		public double sell(boolean enough_share, int stock_id, int share) {
			if (!enough_share)
				return -1;
			int index = stock_id - 1;				
			int qty_after = write_quantity.get(index) + share;
			write_quantity.set(index, qty_after);
			//update to tmp_quantity table
			my_db.to_tmpQty(stock_id, write_quantity, false);
			return price.get(index);
		}
		//exchangeTime ticks
		public void timer_tick() {
			//update table_quantity at exchangeTime = t
			my_db.updateQty(-1, write_quantity, exchangeTime, true);
			
			exchangeTime++;
			//query quantities at exchangeTime = t
			QueryQty();
			
			//query prices at exchangeTime = t
			QueryPrice();
			
			//update write_quantity
			for (int i=0; i<write_quantity.size(); i++) {
				int qty = write_quantity.get(i) + table_quantity.get(i);
				write_quantity.set(i, qty);
			}
		}
}





abstract class Instrument{
	double spotPrice;
	int quantity;
	String name;
}

class Stock extends Instrument{
	
}

class MutualFunds extends Instrument{
	ArrayList<Stock> Portfolio;
}



class Investor{
	Exchange localExchange;
	boolean buyStock(String stockName){
		return true;
	}
	boolean sellStock(String stockName){
		return true;
	}
}

class Company{
	Exchange localExchange;
	String name;
	void issueStock(){
		
	}
}

class Transaction{
	Instrument subject;
	Investor Initiated_by;
	int quantity;
	Time timeStamp;
}

class Address{
	String name;
	String continent;
	String IP;
	int port;
	public Address(String name, String continent, String IP, int port) {
		this.name = name;
		this.continent = continent;
		this.IP = IP;
		this.port = port;
	}
}

class DNSEntry{
	Address address;
	int TTL = 30;
	public DNSEntry(Address address) {
		this.address = address; 
	}
}

class MutualFund{
	String name;
	HashMap<String, Double> proportion;
	public MutualFund(String name, HashMap<String, Double> proportion) {
		this.name = name;
		this.proportion = proportion;
	}
}
