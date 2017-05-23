import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import org.omg.CORBA.TIMEOUT;

public class SuperpeerClient {
	Superpeer superpeer;
	SuperpeerServer serverDelegate;
	static final int TIMEOUT = 100000;
	
	public SuperpeerClient(Superpeer superpeer) {
		this.superpeer = superpeer;
	}
	
//	boolean sendRegister(){
//		if (connectTo(s.housekeeperAddress) == null){
//			return false;
//		}
//		return true;
//	}
	
//------------------------initiative service---------------------------
	Address sendRoute(Address dest, String stockName){
		System.out.println("Routing to " + dest.name + " for " + stockName);
		try (Channel channel = new Channel(dest);){
			if (superpeer.superPeers.containsKey(dest.name))
				channel.output.println("RemoteFind|"+stockName);
			else
				channel.output.println("Find|"+stockName);
			
			channel.socket.setSoTimeout(TIMEOUT);
			String response = channel.input.readLine();
			System.out.println(response);
			String[] contents = response.split("\\|");
			if (contents[1].equals("Success")){
				System.out.println("Found " + stockName + " at " + dest.name);
				return dest;
			}
			else{
				return null;
			}
		}catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	Address sendRemoteRoute(Address dest, String stockName){
		System.out.println("Routing remotely to " + dest.name + " for " + stockName);
		try (Channel channel = new Channel(dest);){
			channel.output.println("RemoteFind|"+stockName);
			channel.socket.setSoTimeout(TIMEOUT);
			String response = channel.input.readLine();
			System.out.println(response);
			String[] contents = response.split("\\|");
			if (contents[1].equals("Success")){
				Address result = new Address(contents[3], contents[2], contents[4], Integer.parseInt(contents[5]));
				return result;
			}
			else{
				return null;
			}
		}catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	//register new exchange coming in, notify "dest" exchange
	void sendNewExchange(Address dest, Address newExchange){
		System.out.println(dest.IP + dest.port);
		try(Channel channel = new Channel(dest);){
			String message = "ExchangeRegistration|" + newExchange.name + "|" + newExchange.IP + "|" + newExchange.port;
			channel.socket.setSoTimeout(TIMEOUT);
			channel.output.println(message);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	boolean sendSuperpeerRegistration(){
		try (
				Channel channel = new Channel(superpeer.houseKeeperAddress);
				){
			String message = "SuperpeerRegistration|" + superpeer.address.continent + "|" + superpeer.address.name + "|"+superpeer.address.IP+"|"+superpeer.address.port;
			System.out.println(message);
			channel.socket.setSoTimeout(TIMEOUT);
			channel.output.println(message);
			String response = channel.input.readLine();
			String[] contents = response.split("\\|");
			if (contents.length > 1 && contents[1].equals("Exists"))
				return false;
			int count = 1;
			while(count < contents.length){
				Address superPeer = new Address(contents[count], contents[count+1], contents[count+2], Integer.parseInt(contents[count+3]));
				superpeer.superPeers.put(superPeer.continent, superPeer);
				System.out.println("Other superpeer received: " + superPeer.name);
				count += 4;
			}
			return true;
			
		} catch (Exception e) {
			System.out.println("Superpeer " + superpeer.address.name + " registration error.");
			e.printStackTrace();
			return false;
		}
		
	}
	
	boolean sendAskElection(Address dest){
		System.out.println("Asking " + dest.name + " to hold a electioin");
		try(
				Channel channel = new Channel(dest);
			){
				channel.output.println("Election");
				channel.socket.setSoTimeout(TIMEOUT);
				String response = channel.input.readLine();
				if (response.equals("ElectionACK"))
					return true;
				return false;
			}
			catch (Exception e) {
				System.out.println("Asking election error");
				return false;
			}
	}
	
	void sendSuperpeerOffline(){
		System.out.println("Superpeer " + superpeer.address.name + " logging off.");
		try(Channel channel = new Channel(superpeer.houseKeeperAddress);){
			String message = "SuperpeerDown|" + superpeer.address.continent + "|" + superpeer.address.name + "|" + superpeer.address.IP + "|" + superpeer.address.port;
			channel.socket.setSoTimeout(TIMEOUT);
			channel.output.println(message);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
//	---------------------------passive service----------------------------------------------
	void sendFindSuccess(Socket s, Address address){
		try (PrintWriter out = new PrintWriter(s.getOutputStream());){
			out.println("Find|Success" + "|" +address.continent+"|"+address.name+"|"+address.IP+"|"+address.port);
			
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	void sendFindFailure(Socket s, Address address){
		try (PrintWriter out = new PrintWriter(s.getOutputStream());){
			out.println("Find|Failure");
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	void sendExchangeRegistrationResponse(Socket socket, String exchangeName){
		String response = "ExchangeRegistrationResponse|";
		try (PrintWriter out = new PrintWriter(socket.getOutputStream());){
			for (String ex : superpeer.innerExchanges.keySet()){
				if(!ex.equals(exchangeName))
					response += superpeer.innerExchanges.get(ex).name+"|"+superpeer.innerExchanges.get(ex).IP+"|"+superpeer.innerExchanges.get(ex).port+"|";
			}
			out.println(response);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	void sendExchangeOffline(Address dest, String name){
		try(
				Channel channel = new Channel(dest);){
			channel.output.println("ExchangeDown|"+name);
		}catch (Exception e) {
			System.out.println("Notifying exchange offline error");
		}
	}
	
	

	
	Socket connectTo (Address dest) throws Exception {
		Socket socket;
		try{
			socket = new Socket(dest.IP, dest.port);
		}catch (Exception e) {
			throw e;
		}
		return socket;
	}
	
	void disconnect(Socket socket){
		try {
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

class Channel implements AutoCloseable{
	Socket socket;
	BufferedReader input;
	PrintWriter output;
	
	public Channel(Address dest) throws Exception{
		socket = new Socket(dest.IP, dest.port);
		input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		output = new PrintWriter(socket.getOutputStream(),true);
	}

	@Override
	public void close() throws Exception {
		output.close();
		input.close();
		socket.close();
	}
}
