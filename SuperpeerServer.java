import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

public class SuperpeerServer extends Thread{
	Superpeer superpeer;
	SuperpeerClient clientDelegate;
	
	public SuperpeerServer(Superpeer superpeer) {
		this.superpeer = superpeer;
	}
	
	public void run() {
		try(
				ServerSocket serverSocket = new ServerSocket(superpeer.address.port);
				){
			while(true){
				Socket receivingSocket = serverSocket.accept();
				new SuperpeerService(receivingSocket,clientDelegate,this).start();
			}
		}catch (Exception e) {
			// TODO: handle exception
		}
	}
}


class SuperpeerService extends Thread{
	SuperpeerClient client;
	SuperpeerServer server;
	Socket socket;
	public SuperpeerService(Socket socket, SuperpeerClient client, SuperpeerServer server) {
		this.socket = socket;
		this.client = client;
		this.server = server;
	}
	
	@Override
	public void run() {
		String command, destExchange, sourceExchange;
		String values[];
		try(
				BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				){
			//extract type information
			command = in.readLine();
			//extract target tag
			destExchange = in.readLine();
			//extract source tag
			sourceExchange = in.readLine();

			//if type is ack
			switch (command) {
			case "Buy":
				
				break;

			default:
				break;
			}
		}catch (IOException e) {
			System.out.println("Reading socket error"+e.toString());
		}
	}
	
}
