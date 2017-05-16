import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.concurrent.TimeoutException;

public class ExchangeClient {
	
	Exchange exchange;
	ExchangeServer serverDelegate;
	
	//-------------------initiative service---------------------
	public ExchangeClient(Exchange exchange) {
		this.exchange = exchange;
	}
	
	boolean sendRegister(){
		if (connectTo(exchange.housekeeperAddress) == null){
			return false;
		}
		return true;
	}
	
	//returns a status code
	//0: success, 1: no inventory, 2: remote exchange not online, 3: stock not exist
	int sendRemoteBuy(Address dest, String stockName, int shares){
		try (Channel channel = new Channel(dest);){
			channel.output.println("ExchangeBuy|"+exchange.address.name+"|"+stockName+"|"+shares);
			channel.socket.setSoTimeout(5000);
			String response = channel.input.readLine();
			String[] contents = response.split("|");
			if (contents[0].equals("ExchangeBuySucceess"))
				return 0;
			else if(contents[2].equals("No inventory"))
				return 1;
			else {
				return 3;
			}
			
		}catch (SocketTimeoutException e) {
			System.out.println("Connection timeout");
			return 2;
		}
		catch (Exception e) {
			e.printStackTrace();
			System.out.println("Sending order error!");
			return 2;
		}
	}
	
	//returns a status code
	//0: success, 1: no inventory, 2: remote exchange not online, 3: stock not exist
	int sendRemoteSell(Address dest, String stockName, int shares){
		try (Channel channel = new Channel(dest);){
			channel.output.println("ExchangeSell|"+exchange.address.name+"|"+stockName+"|"+shares);
			channel.socket.setSoTimeout(5000);
			String response = channel.input.readLine();
			String[] contents = response.split("|");
			if (contents[0].equals("ExchangeSellSucceess"))
				return 0;
			else
				return 3;
		}
		catch (SocketTimeoutException e) {
			System.out.println("Connection timeout");
			return 2;
		}
		catch (Exception e) {
			e.printStackTrace();
			System.out.println("Sending order error!");
			return 2;
		}
	}
	
	Address sendRoute(Address superPeerAddress, String stockName){
		try (Channel channel = new Channel(superPeerAddress);){
			channel.output.println("Find|"+stockName);
			channel.socket.setSoTimeout(5000);
			String response = channel.input.readLine();
			String[] contents = response.split("|");
			if (contents[1].equals("Success"))
			{
				Address ret = new Address(contents[2], contents[3], contents[4], Integer.parseInt(contents[5]));
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
	void sendBuySuccess(Socket s, String userName){
		try (PrintWriter out = new PrintWriter(s.getOutputStream());){
			out.println("BuyResponse|Success");
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	void sendBuyFailure(Socket s, String userName, String reason){
		try (PrintWriter out = new PrintWriter(s.getOutputStream());){
			out.println("BuyResponse|Failure|"+reason);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	void sendSellSuccess(Socket s, String userName){
		try (PrintWriter out = new PrintWriter(s.getOutputStream());){
			out.println("SellResponse|Success");
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	void sendSellFailure(Socket s, String userName, String reason){
		try (PrintWriter out = new PrintWriter(s.getOutputStream());){
			out.println("SellResponse|Success|"+reason);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	void sendExchangeBuySuccess(Socket s, String exchangeName){
		try (PrintWriter out = new PrintWriter(s.getOutputStream());){
			out.println("ExchangeBuyResponse|Success");
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	void sendExchangeBuyFailure(Socket s, String exchangeName, String reason){
		try (PrintWriter out = new PrintWriter(s.getOutputStream());){
			out.println("ExchangeBuyResponse|Failure|"+reason);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	void sendExchangeSellSuccess(Socket s, String exchangeName){
		try (PrintWriter out = new PrintWriter(s.getOutputStream());){
			out.println("ExchangeSellResponse|Success");
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	void sendExchangeSellFailure(Socket s, String exchangeName, String reason){
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
