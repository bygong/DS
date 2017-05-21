import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;


public class ExchangeServer extends Thread{
	
	Exchange exchange;
	ExchangeClient clientDelegate;
	
	public ExchangeServer(Exchange exchange) {
		this.exchange = exchange;
	}
	
	public void run(){
		try(
				ServerSocket serverSocket = new ServerSocket(exchange.address.port);
				){
			while(true){
				Socket receivingSocket = serverSocket.accept();
				new Service(receivingSocket,clientDelegate,this).start();
			}
		}catch (Exception e) {
			// TODO: handle exception
		}
	}
	
}

class Service extends Thread{
	Exchange exchange;
	ExchangeClient client;
	ExchangeServer server;
	Socket socket;
	public Service(Socket socket, ExchangeClient client, ExchangeServer server) {
		this.socket = socket;
		this.client = client;
		this.server = server;
		exchange = server.exchange;
	}
	
	@Override
	public void run() {
		String command, destExchange, sourceExchange;
		String values[];
		try(
				BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				){
			
			String inputString = in.readLine();
			String[] commands = inputString.split("\\|");
			command = commands[0];
			

			switch (command) {
			case "BUY":
				buyHandler(commands[1],commands[2],Integer.parseInt(commands[3]));
				break;
			case "SELL":
				sellHandler(commands[1],commands[2],Integer.parseInt(commands[3]));
				break;
			case "ExchangeBuy":
				exchangeBuyhandler(commands[1],commands[2],Integer.parseInt(commands[3]));
				break;
			case "ExchangeSell":
				exchangeSellhandler(commands[1], commands[2], Integer.parseInt(commands[3]));
				break;
			case "Find":
				findStockHandler(commands[1]);
				break;
			case "TimeSync":
				timeSyncHandler(Integer.parseInt(commands[1]));
				break;
			case "ExchangeRegistration":
				newExchangeHandler(commands[1],commands[2],Integer.parseInt(commands[3]));
			default:
				break;
			}
		}catch (IOException e) {
			System.out.println("Reading socket error"+e.toString());
		}
	}
	
	//handle user buy request
	void buyHandler(String userName, String stockName, int shares){
		if (exchange.mutualFundList.containsKey(stockName)){
			double price = exchange.buyMutualFund(stockName, shares);
			if (price >= 0)
			{
				client.sendBuySuccess(socket,userName,stockName,price);
				return;
			}
		}
		int id = exchange.my_db.QueryStockID((stockName));
		double price;
		//if stock belongs to this exchange
		if (id != -1){
			if ((price = exchange.buy(id,shares)) > 0)
				client.sendBuySuccess(socket,userName,stockName,price);
			else
				//add failure reason
				client.sendBuyFailure(socket,userName,stockName,"");
		}
		else{
			// find which exchange it belongs
			Address dest = exchange.routing(stockName);
			// not found
			if (dest == null)
				client.sendBuyFailure(socket,userName,stockName,"Stock not found");
			else
			{
				price = client.sendRemoteBuy(dest,stockName,shares);
				if (price >= 0)
					client.sendBuySuccess(socket,userName,stockName,price);
				else{
					String reason = "Routing error";
					client.sendBuyFailure(socket,userName,stockName,reason);
				}
			}
		}
	}
	
	//handle user sell request
	void sellHandler(String userName, String stockName, int shares){
		if (exchange.mutualFundList.containsKey(stockName)){
			double price = exchange.sellMutualFund(stockName, shares);
			if (price >= 0)
			{
				client.sendSellSuccess(socket,userName,stockName,price);
				return;
			}
		}
		int id = exchange.my_db.QueryStockID((stockName));
		double price;
		if (id != -1){
			if ((price = exchange.sell(true,id,shares)) > 0)
				client.sendSellSuccess(socket,userName,stockName,price);
			else
				//add failure reason
				client.sendSellFailure(socket,userName,stockName,"");
		}
		else{
			Address dest = exchange.routing(stockName);
			if (dest == null)
				client.sendSellFailure(socket,userName,stockName,"Stock not found");
			else
			{
				price = client.sendRemoteSell(dest,stockName,shares);
				if (price >= 0)
					client.sendSellSuccess(socket,userName, stockName, price);
				else{
					String reason = "Remote Exchange not online";
					client.sendSellFailure(socket,userName,stockName,reason);
				}
			}
		}
	}
	
	void exchangeBuyhandler(String exchangeName, String stockName, int shares){
		int id = exchange.my_db.QueryStockID((stockName));
		double price;
		if (id != -1){
			if ((price = exchange.buy(id,shares)) > 0)
				client.sendExchangeBuySuccess(socket,exchangeName, stockName, price);
			else
				//add failure reason
				client.sendExchangeBuyFailure(socket,exchangeName,stockName,"");
		}
		else
				client.sendExchangeBuyFailure(socket,exchangeName,stockName,"Stock not found");
	}
	
	void exchangeSellhandler(String exchangeName, String stockName, int shares){
		int id = exchange.my_db.QueryStockID((stockName));
		double price;
		if (id != -1){
			if ((price = exchange.sell(true, id,shares)) > 0)
				client.sendExchangeSellSuccess(socket,exchangeName,stockName,price);
			else
				//add failure reason
				client.sendExchangeSellFailure(socket,exchangeName,stockName,"");
		}
		else
				client.sendExchangeSellFailure(socket,exchangeName,stockName,"Stock not found");
	}
	
	void findStockHandler(String stockName){
		if (exchange.stockShelf.containsKey(stockName)){
			client.sendFindSuccess(socket,stockName);
		}else{
			client.sendFindFailure(socket,stockName);
		}
	}
	
	void timeSyncHandler(int timeStamp){
		synchronized (exchange.exchangeTime) {
			exchange.exchangeTime = timeStamp;
		}
		if (!exchange.systemInitiated){
			exchange.systemInitiated = true;
			exchange.timer.scheduleAtFixedRate(exchange.timerTask, 0, Exchange.timeInterval);
		}
	}
	
	void newExchangeHandler(String name, String IP, int port){
		Address newExchange = new Address(name, exchange.address.continent, IP, port);
		exchange.addAddress(name, newExchange);
	}
	
}
