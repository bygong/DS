import java.awt.List;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.channels.*;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Time;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import org.jgroups.JChannel;

public class Exchange{

	public static Address address;
	static ExchangeClient client;
	static ExchangeServer server;
	static Superpeer superpeer;
	static boolean clientInitiated = false, registered = false, systemInitiated = false;
	static Integer exchangeTime = 1;
	static Integer physicalTime = 0;
	
	public DataBase_Connection my_db;			//database of the exchange
	private ArrayList<Float> price;				//price of every stock in the exchange per timer(from table)
	private ArrayList<Integer> table_quantity;	//quantity of every stock in the exchange per timer(from table)
	private ArrayList<Integer> write_quantity;	//quantity written to tmp_quantity table
	static Timer timer = new Timer();
	
	PhysicalTimer timerTask = new PhysicalTimer();
	
	public static final int TIMEOUT = 100000, TIME_INTERVAL = 100, DNS_TIMEOUT = 5;
	
	static Address housekeeperAddress = new Address("Housekeeper", null, "localhost", 8080);
	static Address superPeerAddress;
	
	//address cache pool
	protected static HashMap<String, Address> addressPool = new HashMap<String, Address>();
	protected static HashMap<String, Integer> stockShelf;
	protected static HashMap<String, DNSEntry> dnsTable = new HashMap<>();
	protected static HashMap<String, Address> burnedInExchangeAddresses = new HashMap<String, Address>(){
		{
			put("New_York_Stock_Exchange", new Address("New_York_Stock_Exchange","America","localhost",10001));
			put("Euronext_Paris", new Address("Euronext_Paris","Europe","localhost",10003));
			put("Frankfurt", new Address("Frankfurt","Europe","localhost",10005));
			put("London", new Address("London","Europe","localhost",10007));
			put("Tokyo", new Address("Tokyo","Asia","localhost",10009));
			put("Hong_Kong", new Address("Hong_Kong","Asia","localhost",10011));
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
			put("Sao_Paulo", new Address("Sao_Paulo","America","localhost",10033));
		}
	};
	
	protected static HashMap<String, Address> burnedInSuperpeerAddresses = new HashMap<String, Address>(){
		{
			put("New_York_Stock_Exchange", new Address("New_York_Stock_Exchange","America","localhost",10002));
			put("Euronext_Paris", new Address("Euronext_Paris","Europe","localhost",10004));
			put("Frankfurt", new Address("Frankfurt","Europe","localhost",10006));
			put("London", new Address("London","Europe","localhost",10008));
			put("Tokyo", new Address("Tokyo","Asia","localhost",10010));
			put("Hong_Kong", new Address("Hong_Kong","Asia","localhost",10012));
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
			put("Sao_Paulo", new Address("Sao_Paulo","America","localhost",10034));
		}
	};
	
	protected static HashMap<String, String> initialSuperpeer = new HashMap<String, String>(){
		{
			put("America","New_York_Stock_Exchange");
			put("Europe","Lisbon");
			put("Asia","Seoul");
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
	
	class PhysicalTimer extends TimerTask{
		public void run() {
			synchronized (physicalTime) {
				physicalTime++;
				System.out.println(physicalTime);
				if (physicalTime == TIME_INTERVAL){
					physicalTime = 0;
					exchangeTimeTick();
				}
			}
		}
	}
	
	
	class ExchangeTimer extends Thread{
		@Override
		public void run() {
			synchronized (dnsTable) {
				for (String entry : dnsTable.keySet()){
					if ((dnsTable.get(entry).TTL--) == 0)
						dnsTable.remove(entry);
				}
			}
			synchronized (exchangeTime) {
				exchangeTime++;
			}
			timer_tick();
		}
	}
	
	void exchangeTimeTick(){
		ExchangeTimer exchangeTimeTick = new ExchangeTimer();
		exchangeTimeTick.start();
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
		Exchange exchange = new Exchange(args[0], initialSuperpeer.get(burnedInExchangeAddresses.get(args[0]).continent).equals(args[0]));
		exchange.start();
	}
	
	//constructor, specifying its super peer
	public Exchange(String name, boolean isSuper) {
		my_db = new DataBase_Connection(name);
		write_quantity = new ArrayList<>();
		//initialize write_quantity
		QueryQty();
		QueryPrice();
//		System.err.println(table_quantity.size());
		for (int i=0; i<table_quantity.size(); i++)
			write_quantity.add(table_quantity.get(i));
		
//		System.err.println(write_quantity.size());
		//update to tmp_quantity table
		my_db.to_tmpQty(-1, write_quantity, true);
		
		System.out.println("DB ready.");
		
		address = burnedInExchangeAddresses.get(name);
		if (isSuper)
		{
			becomeSuperpeer();
			superpeer.innerExchanges.put(address.name, address);
			superpeer.run();
		}
		
		addressPool.put(address.name, address);
		server = new ExchangeServer(this);
		client = new ExchangeClient(this);
		
		server.clientDelegate = client;
		client.serverDelegate = server;
		server.start();
		
		
	}
	
	public void start(){
		try (
				BufferedReader userIn = new BufferedReader(new InputStreamReader(System.in));
				){
			
			//create server and client side of this peer
			//server runs as thread while main thread listening on user input
			
			
			
			
			while(!registered)
			{
				registered = client.sendHousekeeperRegister();
			}
			
			registered = false;
			
			System.out.println("???");
//			while(!registered && superPeerAddress != null)
			{
				registered = client.sendRegister();
				
			}

			

		}catch (Exception e) {
			System.out.println(e.toString());
		}
		
		// Trap exit
        Runtime.getRuntime().addShutdownHook(new Thread() {public void run(){
          
        	addressPool.remove(address.name);
        	client.sendLogoff();
        	
        	if (superpeer == null)
        		client.sendOffline();
        	else {
        		superpeer.removeInnerExchange(address.name);
        		superpeer.offline();
        	}
        }});
	}
	
	public boolean hasStock(String stockName) {
		return stockShelf.containsKey(stockName);
	}
	
	
	
	//add a dns entry to my pool
	public void addAddress(String s, Address a){
		System.out.println("Adding " + s + ".");
		synchronized (addressPool) {
			addressPool.put(s, a);
		}
	}
	
	public void removeAddress(String s){
		System.out.println("Removing " + s + ".");
		synchronized (addressPool) {
			addressPool.remove(s);
		}
	}
	
	Address routing(String stockName){
		Address dest;
		
		if (dnsTable.containsKey(stockName))
		{
			System.out.println("DNS cache of \"" + stockName +"\" found: " + dnsTable.get(stockName).address.name);
			return dnsTable.get(stockName).address;
		}
		if (superpeer!=null){
			dest = superpeer.routeTo(stockName);
		}
		else{
			dest = client.sendRoute(superPeerAddress,stockName);
		}
		if (dest != null){
			addDNSEntry(stockName, dest);
		}
		if(dest != null) 	System.out.println(dest.name);
		return dest;
	};
	
	void addDNSEntry(String stockName, Address address){
		synchronized (dnsTable) {
			dnsTable.put(stockName, new DNSEntry(address));
		}
	}
	
	void removeDNSEntry(String stockName){
		synchronized (dnsTable) {
			dnsTable.remove(stockName);
		}
	}
	

	
	boolean isSuperpeer(){
		return superpeer != null;
	}
	
	void becomeSuperpeer(){
		System.out.println("Becoming superpeer...");
		superpeer = new Superpeer(burnedInSuperpeerAddresses.get(address.name),this);
		for (String name : addressPool.keySet()){
			superpeer.addInnerExchange(name, addressPool.get(name));
		}
		boolean success = superpeer.updateInfo();
		if (success)
		{
			System.out.println("Became superpeer.");
			superPeerAddress = null;
		}
		else
		{
			System.out.println("Superpeer already exists, stop becoming superpeer..");
			superpeer = null;
		}
	}
	
	void holdElection(){
		Address result = null;
		while(result == null){
			int num = addressPool.size();
			int count = 1;
			Random random = new Random();
			
			int proposal = random.nextInt(num);
			
			for (String name : addressPool.keySet()){
				if (name != address.name)
				{
					count += client.sendProposal(addressPool.get(name),proposal)? 1 : 0;
				}
			}
			if (count > num / 2){
				ArrayList<Address> samples = new ArrayList<>(addressPool.values());
				result = samples.get(proposal);
			}
		}
		
		for (String name : addressPool.keySet()){
			client.sendCommit(addressPool.get(name), result.name);
		}
		
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
			synchronized (price) {
				synchronized (write_quantity) {
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
			}
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
						price = client.sendRemoteBuy(dest,stock,(int)(shares * fund.proportion.get(stock)));
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
						price = client.sendRemoteBuy(dest,stock,(int)(shares * fund.proportion.get(stock)));
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
			synchronized (write_quantity) {
				if (!enough_share)
					return -1;
				int index = stock_id - 1;				
				int qty_after = write_quantity.get(index) + share;
				write_quantity.set(index, qty_after);
				//update to tmp_quantity table
				my_db.to_tmpQty(stock_id, write_quantity, false);
				return price.get(index);
			}
		}
		//exchangeTime ticks
		public void timer_tick() {
			synchronized (price) {
				synchronized (write_quantity) {
					synchronized (table_quantity) {
//						System.err.println("timer = " + exchangeTime);
						//update table_quantity at timer = t
						my_db.updateQty(-1, write_quantity, exchangeTime-1, true);
						
						//query quantities at timer = t
						QueryQty();
						
						//query prices at timer = ts
						QueryPrice();
						
						//update write_quantity
						for (int i=0; i<write_quantity.size(); i++) {
							int qty = write_quantity.get(i) + table_quantity.get(i);
							write_quantity.set(i, qty);
						}
						
						//update qty_record table
						my_db.to_tmpQty(-1, write_quantity, true);
					}
					
				}
				
			}
		}
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
	int TTL = Exchange.DNS_TIMEOUT;
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
