import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.math.BigInteger;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
//import java.lang.StringBuilder;
/*
 * COMMAND EXAMPLES:
 * insert,eimaste omadara,100
 * insert,Love is in the air,97
 * query,eimaste omadara
 * depart,708
 * delete,Love is in the air
 * delete,akuro_pou_den_uparxei
 * query,*
 */

public class ChordRing {
	
	public static int calculate_sha1(String input, int size) throws NoSuchAlgorithmException{
		MessageDigest mDigest = MessageDigest.getInstance("SHA1");
	    byte[] result = mDigest.digest(input.getBytes());
	    StringBuffer sb = new StringBuffer();
	    for (int i = 0; i < result.length; i++) {
	    	sb.append(String.format("%02x", result[i]));
	    }
	    String asString = sb.toString(); // hexadecimal representation of hash
	    BigInteger value = new BigInteger(asString, 16);
	    value = value.mod(BigInteger.valueOf(size));  
	    return value.intValue();

	}
	
	public static void main_forward_to(String message, int replicas, String hostname, int port){
		//System.err.println(message);
		String message_final;
		String []message_with_replicas = message.split("-");
		if (message_with_replicas.length >= 3 ) message_final = message_with_replicas[0]+"-"+replicas+"-"+message_with_replicas[1]+"-"+message_with_replicas[2];
		else message_final = message;
		Socket socket = null;
		try {
			socket = new Socket(hostname, port);
		} 
		catch (UnknownHostException e) {
		     System.out.println("Unknown host");
		     e.printStackTrace();
		     System.exit(1);
		}
		catch (IOException e) {
			System.out.println("Cannot use this port");
			e.printStackTrace();
		    System.exit(1);
		}
		OutputStream os = null;
		try {
			os = socket.getOutputStream();
		} catch (IOException e) {
			System.out.println("Couldn't get output stream");
			e.printStackTrace();
			System.exit(1);
		}
        OutputStreamWriter osw = new OutputStreamWriter(os);
        BufferedWriter bw = new BufferedWriter(osw);
        try {
        	bw.write(message_final);
            bw.flush();
		} catch (IOException e) {
			System.out.println("Couldn't write to BufferWriter");
			e.printStackTrace();
			System.exit(1);
		}		
	}
	
	public static void main(String[] args) throws IOException {
		
		
		BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
		//System.out.println("Please enter the <number of desired nodes>: ");
		int number_of_nodes = 10;//Integer.parseInt(input.readLine());
		//System.out.println("Please enter the <log of ring size>: ");
		int M = 6; //Integer.parseInt(input.readLine());
		double ring = Math.pow(2,M);
		int ring_size = (int) Math.round(ring);
		int globalc; // global node counter
		//Number of replicas
		int k = 5;
		List<Node> nodelist = new ArrayList<Node>();
		
		System.out.printf("Initial number of nodes: %d ring size: %d replication factor: %d\n",number_of_nodes,ring_size,k);
		
		// create initial ring
		for (globalc=1; globalc<=number_of_nodes; globalc++){
			Node n = new Node("localhost", Integer.toString(globalc), ring_size ,k);
			nodelist.add(n);
		}
		fix_nodes(nodelist);
		/*(for (Node n: nodelist){
			System.out.println(n.getmyId());
		}*/
		for (Node n: nodelist){
			n.start();
		}
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e1) {
			System.out.println("Main couldn't sleep!");
		}
		while(true){
			System.out.println("Type your command: ");
			String command = input.readLine();	
			//System.out.println("command IS: "+ command);
			String option = command.split(",")[0]; //insert,query,delete,join,depart
			if (option.equals("insert") || option.equals("delete") || option.equals("query")) {
				int len = nodelist.size();
				
				int randomNum = ThreadLocalRandom.current().nextInt(0, len-1);
				Node init = nodelist.get(randomNum);
				main_forward_to(command + "-" + init.getMyname()+"-"+ init.getmyPort()+"\n", k, init.getMyname(), init.getmyPort());
			//	main_forward_to("join-"+n.successor.getmyId() +"\n", k, nodelist.get(0).getMyname(), nodelist.get(0).getmyPort());
				System.out.println("Main says: I forwarded the command to Node with ID: " + init.getmyId());
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					System.out.println("Main couldn't sleep!");
				}
			
			}
			else if (option.equals("depart")){
				Node node_to_delete = null;
				int idToDelete = Integer.parseInt(command.split(",")[1]);
				// Check if node with this ID exists
				for (Node n: nodelist){
					if (n.getmyId() == idToDelete){
						node_to_delete = n;
						// send the depart massage
						main_forward_to("depart\n",k, node_to_delete.getMyname(), node_to_delete.getmyPort());
						try {
							Thread.sleep(4000);
						} catch (InterruptedException e) {
							System.out.println("Main couldn't sleep!");
						}
						break;
					}
				}
				if (node_to_delete == null){
					System.out.println("No node with such ID");
				}
				else {
					// fix nodes
					nodelist.remove(node_to_delete);
					fix_nodes(nodelist);
				}
			}
			else if (option.equals("join")){
				Node n = new Node("localhost",Integer.toString(globalc), ring_size,k);
				nodelist.add(n);
				fix_nodes(nodelist);
				n.start();
				// sends join-successorID
				main_forward_to("join-"+n.successor.getmyId() +"\n", k, nodelist.get(0).getMyname(), nodelist.get(0).getmyPort());

				try {
					Thread.sleep(8000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				globalc++;

			}
			
		}
		
		
		
	}

	private static void fix_nodes(List<Node> nodelist) {
		int len = nodelist.size();
		Collections.sort(nodelist);
		for (int i=0; i<len; i++){
			
			if (i == 0){
				// first in ring
				nodelist.get(i).successor = nodelist.get(i+1);
				nodelist.get(i).predecessor = nodelist.get(len - 1);
			}
			else if (i == len - 1){
				// last in ring
				nodelist.get(i).successor = nodelist.get(0);
				nodelist.get(i).predecessor = nodelist.get(i-1);
			}
			else {
				nodelist.get(i).successor = nodelist.get(i+1);
				nodelist.get(i).predecessor = nodelist.get(i-1);
			}
		}
		
	}
}
