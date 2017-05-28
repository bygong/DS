import java.rmi.server.SkeletonNotFoundException;
import java.util.HashMap;

class Superpeer{
	
	// addresses/poo;s
	Address address, houseKeeperAddress = new Address("Housekeeper", null, "localhost", 8080);
	HashMap<String, Address> innerExchanges = new HashMap<>();
	HashMap<String, Address> superPeers = new HashMap<>();
	
	//references
	SuperpeerClient client;
	SuperpeerServer server;
	Exchange exchangeDelegate;
	
	
	
	public Superpeer(Address address1, Exchange e) {
		this.address = address1;
		superPeers = new HashMap<>();
		client = new SuperpeerClient(this);
		server = new SuperpeerServer(this);
		exchangeDelegate = e;
	}
	
	// run superpeer
	public void run(){
		
		client.serverDelegate = server;
		server.clientDelegate = client;
		server.start();
		
	}
	
	// find a stock
	public Address routeTo(String stockName){
		Address address;
		System.out.println("routing for " + stockName);
		if ((address = routeInner(stockName)) != null){
			return address;
		}
		else{
			address = routeOuter(stockName);
			return address;
		}
	}
	
	// route to other continent
	public Address routeOuter(String stockName){
		Address result;	
		for (String peer : superPeers.keySet()){
			if (peer == exchangeDelegate.address.name)
				continue;
			if ((result = client.sendRemoteRoute(superPeers.get(peer),stockName)) != null)
				return result;	
		}
		
		return null;
		
	}
	
	// route in this continent
	public Address routeInner(String stockName){
		Address result;
		if (exchangeDelegate.my_db.QueryStockID(stockName) != -1)
			return exchangeDelegate.address;
		
		for (String peer : innerExchanges.keySet()){
			if (peer == exchangeDelegate.address.name)
				continue;
			if ((result = client.sendRoute(innerExchanges.get(peer),stockName)) != null)
				return result;	
		}
		return null;
	}
	
	
	// a new exchange, notify other exchanges
	void RegisterExchange(Address newExchange){
		for (String ex : innerExchanges.keySet()){
			if(!ex.equals(newExchange.name))
				client.sendNewExchange(innerExchanges.get(ex), newExchange);
		}
		addInnerExchange(newExchange.name, newExchange);
	}
	
	void addInnerExchange(String name, Address address){
		System.out.println("New exchange in this domain: " + name);
		innerExchanges.put(name, address);
	}
	
	void removeInnerExchange(String name){
		System.out.println("Removing exchange in this domain: " + name);
		innerExchanges.remove(name);
	} 
	
	void addSuperpeer(String name, Address address){
		System.out.println("New superpeer received: " + name);
		superPeers.put(name, address);
	}
	
	//fetching superpeer info from housekeeper
	boolean updateInfo(){
		return client.sendSuperpeerRegistration();
	}
	
	//superpeer goes down
	void offline(){
		client.sendSuperpeerOffline();
		// tell exchanges in this continent to remove this exchange
		for (String name : innerExchanges.keySet()){
			if (name != address.name)
				client.sendExchangeOffline(innerExchanges.get(name), address.name);
		}
		
		// find someone to hold election
		for (String name : innerExchanges.keySet()){
			boolean success = client.sendAskElection(innerExchanges.get(name));
			if (success)
				break;
		}
	}
}