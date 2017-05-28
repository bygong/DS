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
	
	//time setting
	public static final int TIMEOUT = 100000, TIME_INTERVAL = 120, DNS_TIMEOUT = 2;
	
	//functional object reference
	static ExchangeClient client;
	static ExchangeServer server;
	static Superpeer superpeer;			//superpeer reference
	static Timer timer = new Timer();
	PhysicalTimer timerTask = new PhysicalTimer();
	
	//local fields, flag, time, addresses
	static boolean registered = false, systemInitiated = false;
	static Integer exchangeTime = 1;	//logical time
	static Integer physicalTime = 0;	//actual time
	public static Address address;		//host address
	static Address housekeeperAddress = new Address("Housekeeper", null, "localhost", 8080);	//hardcoded housekeeper address
	static Address superPeerAddress;	//superpeer of this continent
	
	//database
	public DataBase_Connection my_db;			//database of the exchange
	private ArrayList<Float> price;				//price of every stock in the exchange per timer(from table)
	private ArrayList<Integer> table_quantity;	//quantity of every stock in the exchange per timer(from table)
	private ArrayList<Integer> write_quantity;	//quantity written to tmp_quantity table
	

	
	//address pools
	protected static HashMap<String, Address> addressPool = new HashMap<String, Address>();		//exchange in current continent
	protected static HashMap<String, DNSEntry> dnsTable = new HashMap<>();						//dns cache
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
	};//fixed exchange addresses to simplify testing
	
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
	};//fixed superpeer addresses to simplify testing
	
	protected static HashMap<String, String> initialSuperpeer = new HashMap<String, String>(){
		{
			put("America","New_York_Stock_Exchange");
			put("Europe","Lisbon");
			put("Asia","Seoul");
		}
	};//fixed initial superpeer of each continent
	
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
	};//mutual funds constitution
	
	//Timer for actual time
	class PhysicalTimer extends TimerTask{
		public void run() {
			synchronized (physicalTime) {
				physicalTime++;
				if (physicalTime == TIME_INTERVAL){
					physicalTime = 0;
					exchangeTimeTick();
				}
			}
		}
	}
	
	//Timer for logical timer
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
	
	// logical timer as thread
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
	//data initializing
	public Exchange(String name, boolean isDefaultSuper) {
		my_db = new DataBase_Connection(name);
		write_quantity = new ArrayList<>();
		
		//initialize write_quantity
		table_quantity = my_db.from_tmpQty();
		QueryPrice();
		for (int i=0; i<table_quantity.size(); i++)
			write_quantity.add(table_quantity.get(i));
		
		System.out.println("DB ready.");
		
		//initiating  superpeer
		address = burnedInExchangeAddresses.get(name);
		if (isDefaultSuper)
			becomeSuperpeer();
		
		// put itself in the address pool
		addressPool.put(address.name, address);
		
		//kick off functional parts
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
				// register to both housekeeper and superpeer
				registered = client.sendHousekeeperRegister();
				registered = client.sendRegister();			

		}catch (Exception e) {
			System.out.println(e.toString());
		}
		
		// Trap exit
        Runtime.getRuntime().addShutdownHook(new Thread() {public void run(){
          
        	//remove itself
        	addressPool.remove(address.name);
        	
        	//tell housekeeper it's going offline
        	client.sendLogoff();
        	
        	//if not superpeer, tell superpeer
        	if (superpeer == null)
        		client.sendOffline();
        	//else, superpeer also offline
        	else {
        		superpeer.removeInnerExchange(address.name);
        		superpeer.offline();
        	}
        }});
	}
	
	//add a new exchange in address pool
	public void addAddress(String s, Address a){
		System.out.println("Adding " + s + ".");
		synchronized (addressPool) {
			addressPool.put(s, a);
		}
	}
	
	//remove exchange in address pool
	public void removeAddress(String s){
		System.out.println("Removing " + s + ".");
		synchronized (addressPool) {
			addressPool.remove(s);
		}
	}
	
	// find the exchange of a stockname
	Address routing(String stockName){
		Address dest;
		
		//if it can be found in DNS cache
		if (dnsTable.containsKey(stockName))
		{
			System.out.println("DNS cache of \"" + stockName +"\" found: " + dnsTable.get(stockName).address.name);
			return dnsTable.get(stockName).address;
		}
		
		//if I'm superpeer, route it directly
		if (superpeer!=null){
			dest = superpeer.routeTo(stockName);
		}
		//else send the superpeer a routing request
		else{
			dest = client.sendRoute(superPeerAddress,stockName);
		}
		
		//if found, add it to dns cache
		if (dest != null){
			addDNSEntry(stockName, dest);
		}
//		if(dest != null) 	System.out.println(dest.name);
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
	
	//make itself superpeer
	boolean becomeSuperpeer(){
		System.out.println("Becoming superpeer...");
		superpeer = new Superpeer(burnedInSuperpeerAddresses.get(address.name),this);
		
		//get to know all its nodes
		for (String name : addressPool.keySet()){
			superpeer.addInnerExchange(name, addressPool.get(name));
		}
		
		//get superpeer info from housekeeper
		boolean success = superpeer.updateInfo();
		if (success)
		{
			System.out.println("Became superpeer.");
			superPeerAddress = null;
			superpeer.innerExchanges.put(address.name, address);
			superpeer.run();
		}
		else
		{
			System.out.println("Superpeer already exists, stop becoming superpeer..");
			superpeer = null;
		}
		return success;
	}
	
	//this exchange hold an election
	void holdElection(){
		Address result = null;
		
		//keep electing until a proposal committed
		while(result == null){
			// choose an exchange randomly as proposal
			int num = addressPool.size();
			int count = 1;
			Random random = new Random();
			
			int proposal = random.nextInt(num);
			
			// send proposal, collect returns
			for (String name : addressPool.keySet()){
				if (name != address.name)
				{
					count += client.sendProposal(addressPool.get(name),proposal)? 1 : 0;
				}
			}
			
			//if majority, commit
			if (count > num / 2){
				ArrayList<Address> samples = new ArrayList<>(addressPool.values());
				result = samples.get(proposal);
			}
		}
		
		//commit to other exchanges
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
