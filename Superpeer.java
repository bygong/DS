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
		Address result;
		for (String peer : superPeers.keySet()){
			if (peer == exchangeDelegate.address.name)
				continue;
			if ((result = client.sendRoute(superPeers.get(peer),stockName)) != null)
				return result;	
		}
		return null;
		
	}
	
	void RegisterExchange(String newExchange){
		for (String ex : innerExchanges.keySet()){
			client.sendNewExchange(innerExchanges.get(ex), newExchange);
		}
	}
	
	void updateInfo(){
		client.sendSuperpeerRegistration();
	}
}