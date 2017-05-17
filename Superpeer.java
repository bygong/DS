import java.rmi.server.SkeletonNotFoundException;
import java.util.HashMap;

class Superpeer{
	
	Address address, houseKeeperAddress = new Address("Housekeeper", null, "localhost", 10000);
	HashMap<String, Address> innerExchanges;
	HashMap<String, Address> superPeers;
	
	SuperpeerClient client;
	SuperpeerServer server;
	
	Exchange exchangeDelegate;
	
	public Superpeer(Address address1) {
		this.address = address1;
		superPeers = new HashMap<>();
		updateInfo();
	}
	
	public void run(){
		client = new SuperpeerClient(this);
		server = new SuperpeerServer(this);
		
		client.serverDelegate = server;
		server.clientDelegate = client;
		
		server.start();
		
	}
	
	public Address routeTo(String stockName){
		Address address;
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
			if ((result = client.sendRoute(superPeers.get(peer),stockName)) != null)
				return result;	
		}
		
		return null;
		
	}
	
	public Address routeInner(String stockName){
		Address result;
		if (exchangeDelegate.stockShelf.containsKey(stockName))
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
			client.sendNewExchange(innerExchanges.get(ex), newExchange);
		}
		addInnerExchange(newExchange.name, newExchange);
	}
	
	void addInnerExchange(String name, Address address){
		innerExchanges.put(name, address);
	}
	
	void updateInfo(){
		client.sendSuperpeerRegistration();
	}
}