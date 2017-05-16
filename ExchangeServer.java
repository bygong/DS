import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

import Exchange.ExchangeTimer;

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
			//extract type information
//			command = in.readLine();
//			//extract target tag
//			destExchange = in.readLine();
//			//extract source tag
//			sourceExchange = in.readLine();
			
			String inputString = in.readLine();
			String[] commands = inputString.split("|");
			command = commands[0];
			

			//if type is ack
			switch (command) {
			case "Buy":
				buyHandler(commands[1],commands[2],Integer.parseInt(commands[3]));
				break;
			case "ExchangeBuy":
				exchangeBuyhandler(commands[1],commands[2],Integer.parseInt(commands[3]));
				break;
			case "Find":
				findStockHandler(commands[1]);
				break;
			case "TimeSync":
				timeSyncHandler(Integer.parseInt(commands[1]));
				break;
			default:
				break;
			}
		}catch (IOException e) {
			System.out.println("Reading socket error"+e.toString());
		}
	}
	
	//handle user buy request
	void buyHandler(String userName, String stockName, int shares){
		//if stock belongs to this exchange
		if (exchange.hasStock(stockName)){
			if (buy(stockName,shares))
				client.sendBuySuccess(socket,userName);
			else
				//add failure reason
				client.sendBuyFailure(socket,userName,"");
		}
		else{
			// find which exchange it belongs
			Address dest = exchange.routing(stockName);
			// not found
			if (dest == null)
				client.sendBuyFailure(socket,userName,"Stock not found");
			else
			{
				int orderStatus = client.sendRemoteBuy(dest,stockName,shares);
				if (orderStatus==0)
					client.sendBuySuccess(socket,userName);
				else{
					String reason;
					if (orderStatus==1)	reason = "No inventory";
					else if (orderStatus == 2)	reason = "Remote Exchange not online";
					else reason = "Routing error";
					client.sendBuyFailure(socket,userName,reason);
				}
			}
		}
	}
	
	//handle user sell request
	void sellHandler(String userName, String stockName, int shares){
		if (exchange.hasStock(stockName)){
			if (sell(stockName,shares))
				client.sendSellSuccess(socket,userName);
			else
				//add failure reason
				client.sendSellFailure(socket,userName,"");
		}
		else{
			Address dest = exchange.routing(stockName);
			if (dest == null)
				client.sendSellFailure(socket,userName,"Stock not found");
			else
			{
				int orderStatus = client.sendRemoteSell(dest,stockName,shares);
				if (orderStatus==0)
					client.sendSellSuccess(socket,userName);
				else{
					String reason;
					if (orderStatus==3)	reason = "Routing error";
					else if (orderStatus == 2)	reason = "Remote Exchange not online";
					client.sendSellFailure(socket,userName,reason);
				}
			}
		}
	}
	
	void exchangeBuyhandler(String exchangeName, String stockName, int shares){
		if (exchange.hasStock(stockName)){
			if (buy(stockName,shares))
				client.sendExchangeBuySuccess(socket,exchangeName);
			else
				//add failure reason
				client.sendExchangeBuyFailure(socket,exchangeName,"");
		}
		else
				client.sendExchangeBuyFailure(socket,exchangeName,"Stock not found");
	}
	
	void exchangeSellhandler(String exchangeName, String stockName, int shares){
		if (exchange.hasStock(stockName)){
			if (sell(stockName,shares))
				client.sendExchangeSellSuccess(socket,exchangeName);
			else
				//add failure reason
				client.sendExchangeSellFailure(socket,exchangeName,"");
		}
		else
				client.sendExchangeSellFailure(socket,exchangeName,"Stock not found");
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
}
