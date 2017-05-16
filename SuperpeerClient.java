import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class SuperpeerClient {
	Superpeer superpeer;
	SuperpeerServer serverDelegate;
	
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
		try (Channel channel = new Channel(dest);){
			channel.output.println("Find|"+stockName);
			channel.socket.setSoTimeout(5000);
			String response = channel.input.readLine();
			String[] contents = response.split("|");
			if (contents[1].equals("Success")){
				Address ret = new Address(contents[2], contents[3], contents[4], Integer.parseInt(contents[5]));
				return ret;
			}
			else{
				return null;
			}
		}catch (Exception e) {
			return null;
		}
	}
	
	//register new exchange coming in, notify "dest" exchange
	void sendNewExchange(Address dest, String newExchange){
		
	}
	
	void sendSuperpeerRegistration(){
		try (
				Channel channel = new Channel(superpeer.houseKeeperAddress);
				){
			String message = "SuperpeerRegistration|" + superpeer.houseKeeperAddress.name + "|" + superpeer.address.name;
			channel.output.println(message);
			String response = channel.input.readLine();
		} catch (Exception e) {
			System.out.println("Superpeer " + superpeer.address.name + " registration error.");
			return;
		}
		
	}
//	---------------------------passive service----------------------------------------------
	void sendFindSuccess(Socket s, Address address){
		try (PrintWriter out = new PrintWriter(s.getOutputStream());){
			out.println("RemoteFind|Success" + address.continent+"|"+address.name+"|"+address.IP+"|"+address.port);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	void sendFindFailure(Socket s, Address address){
		try (PrintWriter out = new PrintWriter(s.getOutputStream());){
			out.println("RemoteFind|Failure");
		}catch (Exception e) {
			e.printStackTrace();
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
