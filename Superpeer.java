import java.rmi.server.SkeletonNotFoundException;
import java.util.HashMap;

class Superpeer{
	
	Address address, houseKeeperAddress = new Address("Housekeeper", null, "localhost", 8080);
	HashMap<String, Address> innerExchanges = new HashMap<>();
	HashMap<String, Address> superPeers = new HashMap<>();
	
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
	
	public void run(){
		
		client.serverDelegate = server;
		server.clientDelegate = client;
		
		server.start();
		
	}
	
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
		innerExchanges.remove(name);
	} 
	
	void addSuperpeer(String name, Address address){
		System.out.println("New superpeer received: " + name);
		superPeers.put(name, address);
	}
	
	boolean updateInfo(){
		return client.sendSuperpeerRegistration();
	}
	
	void offline(){
		client.sendSuperpeerOffline();
		
		for (String name : innerExchanges.keySet()){
			if (name != address.name)
				client.sendExchangeOffline(innerExchanges.get(name), address.name);
		}
		
		for (String name : innerExchanges.keySet()){
			boolean success = client.sendAskElection(innerExchanges.get(name));
			if (success)
				break;
		}
	}
}