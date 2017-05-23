import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.concurrent.TimeoutException;

public class ExchangeClient {
	
	Exchange exchange;
	ExchangeServer serverDelegate;
	static final int TIMEOUT = 100000;
	
	//-------------------initiative service---------------------
	public ExchangeClient(Exchange exchange) {
		this.exchange = exchange;
	}
	
	boolean sendRegister(){
		try (Channel channel = new Channel(exchange.superPeerAddress);){
			channel.output.println("ExchangeRegistration|"+exchange.address.name +"|" + exchange.address.IP + "|" + exchange.address.port);
			channel.socket.setSoTimeout(TIMEOUT);
			String response = channel.input.readLine();
			System.out.println(response);
			String[] contents = response.split("\\|");
			if (contents[0].equals("ExchangeRegistrationResponse"))
			{
				int count = 1;
				while(count < contents.length){
					Address otherExchange = new Address(contents[count], exchange.address.continent, contents[count+1], Integer.parseInt(contents[count+2]));
					exchange.addAddress(otherExchange.name, otherExchange);
					count += 3;
				}
				return true;
			}
			else{
				return false;
			}
		}catch (Exception e) {
			System.out.println("register to super peer error");
			e.printStackTrace();
			return false;
		}
	}
	
	boolean sendHousekeeperRegister(){
		try (Channel channel = new Channel(exchange.housekeeperAddress);){
			channel.output.println("ExchangeRegistration|"+exchange.address.name +"|" +exchange.address.continent + "|" + exchange.address.IP + "|" + exchange.address.port);
			channel.socket.setSoTimeout(TIMEOUT);
			String response = channel.input.readLine();
			System.out.println(response);
			String[] contents = response.split("\\|");
			if (contents[0].equals("ExchangeRegistrationResponse"))
			{
				String superpeerName = contents[1], superpeerIP = contents[2];
				int superpeerPort = Integer.parseInt(contents[3]);
				exchange.superPeerAddress = new Address(superpeerName, exchange.address.continent, superpeerIP, superpeerPort);
				return true;
			}
			else{
				return false;
			}
		}catch (Exception e) {
			System.out.println("register to housekeeper error");
			return false;
		}
	}
	
	//returns a status code
	//0: success, 1: no inventory, 2: remote exchange not online, 3: stock not exist
	double sendRemoteBuy(Address dest, String stockName, int shares){
		try (Channel channel = new Channel(dest);){
			channel.output.println("ExchangeBuy|"+exchange.address.name+"|"+stockName+"|"+shares);
			channel.socket.setSoTimeout(TIMEOUT);
			String response = channel.input.readLine();
			System.out.println(response);
			String[] contents = response.split("\\|");
			if (contents[1].equals("Success"))
			{
				double price = Double.parseDouble(contents[2]);
				return price;
			}
			else
				return -1;
			
		}catch (Exception e) {
			e.printStackTrace();
			System.out.println("Sending order error!");
			return -1;
		}
	}
	
	//returns a status code
	//0: success, 1: no inventory, 2: remote exchange not online, 3: stock not exist
	double sendRemoteSell(Address dest, String stockName, int shares){
		try (Channel channel = new Channel(dest);){
			channel.output.println("ExchangeSell|"+exchange.address.name+"|"+stockName+"|"+shares);
			channel.socket.setSoTimeout(TIMEOUT);
			String response = channel.input.readLine();
			System.out.println(response);
			String[] contents = response.split("\\|");
			if (contents[1].equals("Success"))
			{
				double price = Double.parseDouble(contents[2]);
				return price;
			}else
				return -1;
		}catch (Exception e) {
			e.printStackTrace();
			System.out.println("Sending order error!");
			return -1;
		}
	}
	
	Address sendRoute(Address superPeerAddress, String stockName){
		try (Channel channel = new Channel(superPeerAddress);){
			System.out.println("Sending routing of " +stockName);
			channel.output.println("Find|"+stockName);
			channel.socket.setSoTimeout(TIMEOUT);
			String response = channel.input.readLine();
			System.out.println(response);
			String[] contents = response.split("\\|");
			if (contents[1].equals("Success"))
			{
				Address ret = new Address(contents[3], contents[2], contents[4], Integer.parseInt(contents[5]));
				return ret;
			}
			else{
				return null;
			}
		}catch (Exception e) {
			System.out.println("sending route error");
			return null;
		}
		
	}
	
	//------------------passive service-------------------------
	void sendBuySuccess(Socket s, String userName, String stockName, double price){
		try (PrintWriter out = new PrintWriter(s.getOutputStream());){
			out.println("BuyResponse|Success|"+price);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	void sendBuyFailure(Socket s, String userName, String stockName, String reason){
		try (PrintWriter out = new PrintWriter(s.getOutputStream());){
			out.println("BuyResponse|Failure|"+reason);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	void sendSellSuccess(Socket s, String stockName, String userName, double price){
		try (PrintWriter out = new PrintWriter(s.getOutputStream());){
			out.println("SellResponse|Success|"+price);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	void sendSellFailure(Socket s, String userName, String stockName, String reason){
		try (PrintWriter out = new PrintWriter(s.getOutputStream());){
			out.println("SellResponse|Failure|"+reason);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	void sendExchangeBuySuccess(Socket s, String exchangeName, String stockName, double price){
		try (PrintWriter out = new PrintWriter(s.getOutputStream());){
			out.println("ExchangeBuyResponse|Success|"+price);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	void sendExchangeBuyFailure(Socket s, String exchangeName, String stockName, String reason){
		try (PrintWriter out = new PrintWriter(s.getOutputStream());){
			out.println("ExchangeBuyResponse|Failure|"+reason);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	void sendExchangeSellSuccess(Socket s, String exchangeName, String stockName, double price){
		try (PrintWriter out = new PrintWriter(s.getOutputStream());){
			out.println("ExchangeSellResponse|Success|"+price);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	void sendExchangeSellFailure(Socket s, String exchangeName, String stockName, String reason){
		try (PrintWriter out = new PrintWriter(s.getOutputStream());){
			out.println("ExchangeSellResponse|Failure|"+reason);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	void sendFindSuccess(Socket s, String stockName){
		try (PrintWriter out = new PrintWriter(s.getOutputStream());){
			out.println("Find|Success");
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	void sendFindFailure(Socket s, String stockName){
		try (PrintWriter out = new PrintWriter(s.getOutputStream());){
			out.println("Find|Failure");
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	void sendOffline(){
		try(
				Channel channel = new Channel(exchange.superPeerAddress);
				){
			channel.output.println("ExchangeOffline|"+exchange.address.name);
		}catch (Exception e) {
			System.out.println("Offline error");
		}
		
	}
	
	void sendLogoff(){
		try(
				Channel channel = new Channel(exchange.housekeeperAddress);
			){
				channel.output.println("ExchangeLogoff|" + exchange.address.name);
			}catch (Exception e) {
				System.out.println("Logging off error");
			}
	}
	
	
	Socket connectTo(Address dest){
		Socket socket;
		try{
			socket = new Socket(dest.IP, dest.port);
		}catch (Exception e) {
			return null;
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
