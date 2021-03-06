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
	
	// register to superpeer
	boolean sendRegister(){
		try (Channel channel = new Channel(exchange.superPeerAddress);){
			
			channel.output.println("ExchangeRegistration|"+exchange.address.name +"|" + exchange.address.IP + "|" + exchange.address.port);
			channel.socket.setSoTimeout(TIMEOUT);
			String response = channel.input.readLine();
			String[] contents = response.split("\\|");
			if (contents[0].equals("ExchangeRegistrationResponse"))
			{
				int count = 1;
				// receiving other exchange in this continent
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
			return false;
		}
	}
	
	// register to housekeeper
	boolean sendHousekeeperRegister(){
		try (Channel channel = new Channel(exchange.housekeeperAddress);){
			channel.output.println("ExchangeRegistration|"+exchange.address.name +"|" +exchange.address.continent + "|" + exchange.address.IP + "|" + exchange.address.port);
			channel.socket.setSoTimeout(TIMEOUT);
			String response = channel.input.readLine();
			String[] contents = response.split("\\|");
			if (contents[0].equals("ExchangeRegistrationResponse"))
			{
				String superpeerName = contents[1], superpeerIP = contents[2];
				int superpeerPort = Integer.parseInt(contents[3]);
				
				//being told the current superpeer
				exchange.superPeerAddress = new Address(superpeerName, exchange.address.continent, superpeerIP, superpeerPort);
				
				// contents length == 5 indicates that system is already running upon this exchange coming online
				// so housekeeper will tell it the current time stamp
				if (contents.length == 5)
				{
					int timeStamp = Integer.parseInt(contents[4]);
					System.out.println("Time synchronized: " +timeStamp);
					synchronized (exchange.physicalTime) {
						exchange.physicalTime = 0;
					}
					
					if (!exchange.systemInitiated){
						exchange.systemInitiated = true;
						exchange.timer.scheduleAtFixedRate(exchange.timerTask, 0, 1000);
					}
					
					synchronized (exchange.exchangeTime) {
						if (timeStamp > exchange.exchangeTime){
							exchange.exchangeTime = timeStamp;
							exchange.exchangeTimeTick();
						}
					}
				}
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
	//non-negative: price, -1: failure
	double sendRemoteBuy(Address dest, String stockName, int shares){
		try (Channel channel = new Channel(dest);){
			channel.output.println("ExchangeBuy|"+exchange.address.name+"|"+stockName+"|"+shares);
			channel.socket.setSoTimeout(TIMEOUT);
			String response = channel.input.readLine();
			String[] contents = response.split("\\|");
			if (contents[1].equals("Success"))
			{
				double price = Double.parseDouble(contents[2]);
				return price;
			}
			else
				return -1;
			
		}catch (Exception e) {
			System.out.println("Sending order error!");
			return -1;
		}
	}
	
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
			System.out.println("Sending order error!");
			return -1;
		}
	}
	
	//sending superpeer routing request
	Address sendRoute(Address superPeerAddress, String stockName){
		try (Channel channel = new Channel(superPeerAddress);){
			System.out.println("Sending routing of " +stockName + " to " + superPeerAddress.name);
			channel.output.println("Find|"+stockName);
			channel.socket.setSoTimeout(TIMEOUT);
			String response = channel.input.readLine();
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
	
	boolean sendProposal(Address dest, int proposal){
		try (Channel channel = new Channel(dest);){
			System.out.println("Sending proposal " + proposal + " to " + dest.name);
			channel.output.println("Proposal|"+proposal);
			channel.socket.setSoTimeout(TIMEOUT);
			String response = channel.input.readLine();
			String[] contents = response.split("\\|");
			if (contents[1].equals("Accept"))
			{
				return true;
			}
			else{
				return false;
			}
		}catch (TimeoutException e) {
			System.out.println(dest.name + " not responding.");
			return false;
		}
		catch (Exception e) {
			System.out.println("sending route error");
			return false;
		}
	}
	
	void sendCommit(Address dest, String name){
		try (Channel channel = new Channel(dest);){
			System.out.println("Sending commit to " + dest.name);
			channel.output.println("ProposalCommit|"+name);
		}
		catch (Exception e) {
			System.out.println("sending route error");
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
				channel.output.println("ExchangeOffline|" + exchange.address.name);
			}catch (Exception e) {
				System.out.println("Logging off error");
			}
	}
	
	//------------------passive service-------------------------
	void sendBuySuccess(Socket s, String userName, String stockName, double price){
		try (PrintWriter out = new PrintWriter(s.getOutputStream());){
			out.println("BuyResponse|Success|"+price);
		}catch (Exception e) {
		}
	}
	
	void sendBuyFailure(Socket s, String userName, String stockName, String reason){
		try (PrintWriter out = new PrintWriter(s.getOutputStream());){
			out.println("BuyResponse|Failure|"+reason);
		}catch (Exception e) {
		}
	}
	
	void sendSellSuccess(Socket s, String stockName, String userName, double price){
		try (PrintWriter out = new PrintWriter(s.getOutputStream());){
			out.println("SellResponse|Success|"+price);
		}catch (Exception e) {
		}
	}
	
	void sendSellFailure(Socket s, String userName, String stockName, String reason){
		try (PrintWriter out = new PrintWriter(s.getOutputStream());){
			out.println("SellResponse|Failure|"+reason);
		}catch (Exception e) {
		}
	}
	
	void sendExchangeBuySuccess(Socket s, String exchangeName, String stockName, double price){
		try (PrintWriter out = new PrintWriter(s.getOutputStream());){
			out.println("ExchangeBuyResponse|Success|"+price);
		}catch (Exception e) {
		}
	}
	
	void sendExchangeBuyFailure(Socket s, String exchangeName, String stockName, String reason){
		try (PrintWriter out = new PrintWriter(s.getOutputStream());){
			out.println("ExchangeBuyResponse|Failure|"+reason);
		}catch (Exception e) {
		}
	}
	
	void sendExchangeSellSuccess(Socket s, String exchangeName, String stockName, double price){
		try (PrintWriter out = new PrintWriter(s.getOutputStream());){
			out.println("ExchangeSellResponse|Success|"+price);
		}catch (Exception e) {
		}
	}
	
	void sendExchangeSellFailure(Socket s, String exchangeName, String stockName, String reason){
		try (PrintWriter out = new PrintWriter(s.getOutputStream());){
			out.println("ExchangeSellResponse|Failure|"+reason);
		}catch (Exception e) {
		}
	}
	
	void sendFindSuccess(Socket s, String stockName){
		try (PrintWriter out = new PrintWriter(s.getOutputStream());){
			out.println("Find|Success");
		}catch (Exception e) {
		}
	}
	
	void sendFindFailure(Socket s, String stockName){
		try (PrintWriter out = new PrintWriter(s.getOutputStream());){
			out.println("Find|Failure");
		}catch (Exception e) {
		}
	}
	
	
	
	void sendProposalAccept(Socket socket){
		try (PrintWriter out = new PrintWriter(socket.getOutputStream());){
			out.println("Proposal|Accept");
		}catch (Exception e) {
		}
	}
	
	void sendProposalReject(Socket socket){
		try (PrintWriter out = new PrintWriter(socket.getOutputStream());){
			out.println("Proposal|Reject");
		}catch (Exception e) {
		}
	}
	
	void replyElectionRequest(Socket socket){
		try (PrintWriter out = new PrintWriter(socket.getOutputStream());){
			out.println("ElectionACK");
		}catch (Exception e) {
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
		}
	}
}
