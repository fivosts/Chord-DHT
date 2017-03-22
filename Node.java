import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//needed for catch(IOException e)
import java.io.*;
import java.net.ServerSocket;
//needed for socket setup
import java.net.Socket;
import java.net.UnknownHostException;

public class Node extends Thread implements Comparable<Node> {
	public static final int PORT_BASE = 49152;
	private int myid = 0;
	Node successor, predecessor;
	private int myport = 0; // takes values PORT_BASE+1, PORT_BASE+2, ...
	private String myname; // "localhost" here
	private String seira;
	private int global_rep;
	int ring_size;
	private int arrived = 0;
	private boolean IamInit = false;
	Map<String, Integer> files = new HashMap<>();
	

	public Node(String name, String seiratou, int size, int replicasNumbers) {
		myname = name;
		ring_size = size;
		seira = seiratou;
		myport = PORT_BASE + Integer.parseInt(seiratou);
		try {
			myid = ChordRing.calculate_sha1(seira, ring_size);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		global_rep = replicasNumbers;
	}
	
	public String getSeira(){
		return seira;
	}

	private boolean iAmResponsibleForId (int song_id){
		if (song_id > predecessor.getmyId() && song_id <= myid) {
			return true;
		}
		else if (myid < predecessor.getmyId() && (song_id <= myid || song_id > predecessor.getmyId())){
			return true;
		}
		else {
			return false;
		}
	}
	
	public int getmyId() {
			return myid;
	}
	

	public int getmyPort(){
		return myport;
	}
	
	public int getRing_size() {
		return ring_size;
	}

	public void setRing_size(int ring_size) {
		this.ring_size = ring_size;
	}
	
	public String getMyname() {
		return myname;
	}

	public void setMyname(String myname) {
		this.myname = myname;
	}
	
	int query(String key,int counter) throws NoSuchAlgorithmException{
		int song_id = ChordRing.calculate_sha1(key, ring_size);
		int value = -1; // i am not responsible for song
		if (iAmResponsibleForId(song_id) ){
			System.out.println("Node "+ myid + ": I am responsible for : "+ key);
			if (!files.containsKey(key)){
				value = -2; // i am responsible, song doesn't exist 
		
			}
			else value = -3; //  i am responsible, song exists in me
		}
		if (counter < global_rep) value = -3;
		if (counter == 1){
			if (files.containsKey(key)) value = files.get(key);
		}
		return value;
	}
	
	int insert(String key, int value,int counter) throws NoSuchAlgorithmException{
		int song_id = ChordRing.calculate_sha1(key, ring_size);
		int answer = 0; // i am not responsible for song
		if (iAmResponsibleForId(song_id)){
			System.out.println("Node "+ myid + ": I am responsible for :" + key);
			if (files.containsKey(key)){
				// update
				files.replace(key, value);
			}
			else{
				// insert
				files.put(key, value);
			}
			answer = 1; // I did the insert
		}
		else if( counter < global_rep){
			System.out.println("Node "+ myid + ": I am replicating :" + key);
			if (files.containsKey(key)){
				// update
				files.replace(key, value);
			}
			else{
				// insert
				files.put(key, value);
			}
			answer=1;
		}
		return answer;
	}
	
	int delete (String key,int counter) throws NoSuchAlgorithmException{
		int song_id = ChordRing.calculate_sha1(key, ring_size);
		int answer = 0; // i am not responsible for song
		if (iAmResponsibleForId(song_id)){
			System.out.println("Node "+ myid + ": I am responsible for :" + key);
			if (!files.containsKey(key)){
				answer = -2; // i am responsible, song doesn't exist 

			}
			else{
				files.remove(key);
				answer = -3; // I am responsible, song exists in me
		
			}
		}
		if (!iAmResponsibleForId(song_id) && counter < global_rep) {
			files.remove(key);
			answer = -3;
		}
		if (counter == 1){
			if (answer == -3) answer = 1;
		}
		return answer;
	}
	
	public void run () {
		System.out.println("Node-Thread with id " + myid + " and port " + myport + " started!\n");
		/* This function is executed by each thread in Chord Ring. 
		 * It setups a socket for each thread (node) and then waits (remains open
		 * and listens for incoming connections) until a depart query 
		 * for this node arrives. 
		 */
		
		// The port in which the connection is set up. 
		// A valid port value is between 0 and 65535
		ServerSocket serverSocket = null;
		InputStream is = null;
		InputStreamReader isr;
		BufferedReader br = null;
		String message_to_handle = null;
		Socket channel = null;
		int replica_counter = 0;
		/* Creates a Server Socket with the computer name (hostname) 
		 * and port number (port). 
		 * Each node has a server socket in order to send and receive 
		 * queries.
		 */
		
			
		while (true){
			IamInit = false;
			try {
				serverSocket = new ServerSocket(myport);
			} catch (IOException e) {
	            System.err.println("Could not listen on defined port");
	            e.printStackTrace();
	            System.exit(1);
	        }
			try {
				channel = serverSocket.accept();
			} catch (IOException e) {
				System.err.println("Accept failed");
	            System.exit(1);
			}
			try {
				is = channel.getInputStream();
			} catch (IOException e) {
				System.err.println("Getting input stream failed");
	            System.exit(1);
			}
			isr = new InputStreamReader(is);
			br = new BufferedReader(isr);
			
			
			/* Reading the message from the client
			 * Method accept() returns when a client has 
			 * connected to server.
			 */

			// If you read from the input stream, you'll hear what the client has to say.
			try {
				// block until a request has arrived
				message_to_handle = br.readLine();
			} catch (IOException e) {
				System.err.println("ReadLine failed");
	            System.exit(1);
			}	        
			System.out.println("Node "+ myid +": Got message: "+ message_to_handle); //TODO:den einai sosti ayti

	        // decide if I am the initial node who received the query
	        
	        String[] message = message_to_handle.split("-");
	        String theQuery = message[0]; // keeps what the user entered

	        if ((!theQuery.equals("ANSWER")) && (!theQuery.equals("GET_MY_STUFF")) && !(theQuery.equals("depart"))){
	        	replica_counter=Integer.parseInt(message[1]);
	        	System.out.println("Node "+ myid +":"+" Replica_counter is now "+replica_counter);
	        }
	        
	        if (message.length == 4){
	        	if (Integer.parseInt(message[3]) == myport){ // message[3] is the initial host's port
	        		// I am the initial node
	        		//System.out.println("Node "+myid+": I am init");
	        		IamInit = true;
	        	}
	        }
	     // Check if it is an answer
	        if (theQuery.equals("ANSWER")){
	        	
	        	// Initial node prints the answer
	        	System.err.println("Node "+myid+": "+message[1]);
	        	try {
	    			serverSocket.close();
	    		} catch (IOException e) {
	    			e.printStackTrace();
	    		}
	        	continue;
	        }
	        
	        if (message.length == 4){
	        	if (Integer.parseInt(message[3]) == myport){ // message[3] is the initial host's port
	        		// I am the initial node
	        		//System.out.println("Node "+myid+": I am init");
	        		IamInit = true;
	        	}
	        }
	        String []splittedMessage = theQuery.split(",");
	        // decide what to do according to the type of query
	        
	        
	        if (splittedMessage[0].equals("insert")) {
	        	if (splittedMessage.length != 3){
	        		System.err.println("Wrong number of parameters");
	        	}
	        	else {
	        		int insertresult = -1;
					try {
						insertresult = insert(splittedMessage[1], Integer.parseInt(splittedMessage[2]),replica_counter);
						
					} catch (NoSuchAlgorithmException e) {
						e.printStackTrace();
						System.exit(1);
					} 
	        		if (insertresult == 1){
	        			// if I did the insert
	        			String myAnswer = "node "+ myid +" Inserted pair ("+ splittedMessage[1] + "," + splittedMessage[2]+")";
	        			if (IamInit){
	        				//System.err.println(myAnswer); //DEBUGGING
	        				System.err.println("Node "+ myid + ": " + myAnswer);
	        			}
	        			else {
	        				forward_to("ANSWER-"+myAnswer+"\n",replica_counter, message[2], Integer.parseInt(message[3])); //message[2] is the initial host's name
	        			}
	        			try {
        		            // thread to sleep for 1000 milliseconds
        		            sleep(1000);
        		         } catch (Exception e) {
        		            System.out.println(e);
        		         }
	        			if (replica_counter>1){
	        				replica_counter--;
	        				forward_to(message_to_handle+"\n",replica_counter, successor.getMyname(), successor.getmyPort());
	        				
	        			}
	        			else{
	     					System.out.println("Node "+myid+": I am the last thread inserting and going to sleep");
	        			}
	        			
	        		}
	        		else {
	        			// I didn't do the insert
	        			forward_to(message_to_handle+"\n",replica_counter, successor.getMyname(), successor.getmyPort());
        				System.out.println("Node "+myid+": I forwarded the query to "+successor.getMyname()+":"+successor.getmyPort());	    
	        		}
	        	}
	        }
	        else if (splittedMessage[0].equals("query")) {

	        	if (splittedMessage.length != 2){
	        		System.err.println("Wrong number of parameters");
	        	}
	        	else {
	        		
        			int queryresult = -6; 
        			
        			if (!(splittedMessage[1].equals("*"))){
        				try {
							queryresult = query(splittedMessage[1],replica_counter);
	        			}
        				catch (NoSuchAlgorithmException e) {
							e.printStackTrace();
							System.exit(1);
						}
        	//			if(queryresult>0){
//DEBUGGING     			System.out.println(queryresult);
        		//		}
        				if (queryresult == -1){
        					System.out.println("Node "+myid+": queryresult = "+queryresult);
        					//I am not the responsible node to talk about it :/
        					//ask the next one :(
        					forward_to(message_to_handle+"\n",replica_counter, successor.getMyname(), successor.getmyPort());
            				System.out.println("Node "+myid+": I forwarded the query to "+successor.getMyname()+":"+successor.getmyPort());
        				}
        				if (queryresult == -2){
        					System.out.println("Node "+myid+": queryresult = "+queryresult);

        					// I don't have this but i am responsible for this song
        					String answer = "responsible node "+myid+" didn't find "+splittedMessage[1];
        					if (IamInit) System.out.println("Node "+myid+": " +answer);
        					else {
            					forward_to("ANSWER-" +answer+"\n",replica_counter, message[2], Integer.parseInt(message[3]));
            					try {
            			            // thread to sleep for 1000 milliseconds
            			            sleep(1000);
            			         } catch (Exception e) {
            			            System.out.println(e);
            			         }
        					}
        					//System.out.println(splittedMessage[1]+": Not found");
        				}
        				
        				if (queryresult == -3){
        					System.out.println("Node "+myid+": queryresult = "+queryresult);

        					replica_counter--;
	        				forward_to(message_to_handle+"\n",replica_counter, successor.getMyname(), successor.getmyPort());

        				}
        				
        				if (queryresult > 0){
        					System.out.println("Node "+myid+": queryresult = "+queryresult);

        					//file exists in my list
        					String answer = "node " + myid + " said 'I've got this song', value = "+queryresult;
        					
        					if (IamInit) System.err.println("Node "+myid+": " +answer);
        					else{
        						forward_to("ANSWER-"+answer+"\n",replica_counter, message[2], Integer.parseInt(message[3]));
        						try {
        							// thread to sleep for 1000 milliseconds
        							sleep(1000);
        						} 
        						catch (Exception e) {
        				           System.out.println(e);
        						}
        					}
        				}
        				
        				
        			}
        			else{
        				if (IamInit){
        					arrived ++;
        					//System.out.println("Node "+myid+ ":  Arrived = "+arrived);
        					if (arrived == 2){
	        					//stop
	        					arrived=0;
	        					System.out.println("I am initial node.. finished");
	        				}
        					else{
        						// I am init and I print my list
        						for (String key : files.keySet()) {
            					    System.err.println("Node "+myid+": "+key + " " + files.get(key));
        						}
            					forward_to(message_to_handle+"\n",replica_counter, successor.getMyname(), successor.getmyPort());
        					}
        				}
        				else{
        					try {
    				            // thread to sleep for 1000 milliseconds
    				            sleep(1000);
    				         } catch (Exception e) {
    				            System.out.println(e);
    				         }
        					// every node answers with its list
	        				if (!files.isEmpty()){
        						for (String key : files.keySet()) {
	        						forward_to("ANSWER-"+key + " " + files.get(key)+"\n",replica_counter, message[2], Integer.parseInt(message[3]));
	        						//System.out.println("Node "+myid+": forwarded to "+message[2] +":"+message[3]);
	        						try {
	        				            // thread to sleep for 1000 milliseconds
	        				            sleep(500);
	        				         } catch (Exception e) {
	        				            System.out.println(e);
	        				         }
	        					}
	        				}
        					//forward the message to the next node
        					forward_to(message_to_handle+"\n",replica_counter, successor.getMyname(), successor.getmyPort());
        				
        				
        				}
        			}
	        	}
	        }
	        else if (splittedMessage[0].equals("delete")) {

	        	if (splittedMessage.length != 2){
	        		System.err.println("Wrong number of parameters");
	        	}
	        	else {
        			int deleteresult = -6;
        			try {
						deleteresult = delete(splittedMessage[1], replica_counter);
					} catch (NoSuchAlgorithmException e) {
						e.printStackTrace();
					}
        			if (deleteresult == 0){
        				
        				// I am not responsible
        				forward_to(message_to_handle+"\n",replica_counter, successor.getMyname(), successor.getmyPort());
        			}
        			else if (deleteresult == -2){
        				// Song doesn't exist
        				String answer = "Song Doesn't exist :"+ splittedMessage[1];
    					if (IamInit) System.out.println("Node "+myid+": " +answer);
    					else {
    						forward_to("ANSWER-"+answer+"\n",replica_counter, message[2], Integer.parseInt(message[3]));
    						try {
    				            // thread to sleep for 1000 milliseconds
    				            sleep(1000);
    				         } catch (Exception e) {
    				            System.out.println(e);
    				         }
    					}
        			}		
        			else if (deleteresult == -3){
        				
        				// I deleted it
        				try {
        		            // thread to sleep for 1000 milliseconds
        		            sleep(1000);
        		         } catch (Exception e) {
        		            System.out.println(e);
        		         }
        				String answer = "Node "+myid+": Deleted song "+ splittedMessage[1];

    					if (IamInit) {
    						System.err.println("Node "+myid+": " +answer);
    					}
    					else{ 
    						forward_to("ANSWER-"+answer+"\n",replica_counter, message[2], Integer.parseInt(message[3]));
    						try {
    				            // thread to sleep for 1000 milliseconds
    				            sleep(1000);
    				         } catch (Exception e) {
    				            System.out.println(e);
    				         }
    					}
        				replica_counter--;
            			forward_to(message_to_handle+"\n",replica_counter, successor.getMyname(), successor.getmyPort());	
        			}
        			else if (deleteresult ==1){
        				String answer = "Node "+myid+": Deleted song "+ splittedMessage[1];

    					if (IamInit) {
    						System.err.println("Node "+myid+": " +answer);
    					}
    					else{ 
    						forward_to("ANSWER-"+answer+"\n",replica_counter, message[2], Integer.parseInt(message[3]));
    						try {
    				            // thread to sleep for 1000 milliseconds
    				            sleep(1000);
    				         } catch (Exception e) {
    				            System.out.println(e);
    				         }
    					}
        			}
	        	}		
	        }
	        else if (splittedMessage[0].equals("depart")) {
	        	System.out.println("Node "+myid+": in depart");
	        	for (String key : files.keySet()) {
					forward_to("GET_MY_STUFF-"+key + "," + files.get(key)+"\n",replica_counter, successor.getMyname(), successor.getmyPort());
					try {
						sleep(2000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
	        	try {
					serverSocket.close();
				} catch (IOException e) {
					System.out.println("Socket couldn't close during depart!");
				}
	        	System.out.println("Node "+ myid+": closed socket");
	        	break;
	        }
	        else if (splittedMessage[0].equals("GET_MY_STUFF")) {
	        	String key = message[1].split(",")[0];
	        	int value = Integer.parseInt(message[1].split(",")[1]);
	        	files.put(key, value);
	        }
	        else if (message[0].equals("join")) {
	        	int hisSuccessorID = Integer.parseInt(message[1]);
	        	List<String> to_remove = new ArrayList<String>();
	        	if (myid == hisSuccessorID ){
	        		for (String key : files.keySet()) {
	        			//System.out.println(key);
	        			try {
							if (ChordRing.calculate_sha1(key, ring_size) <= predecessor.getmyId()){
								System.out.println("Node "+myid+": " +"forwarding key "+key+ " to "+predecessor.getMyname()+":"+predecessor.getmyPort());
								forward_to("GET_MY_STUFF-"+key + "," + files.get(key)+"\n",replica_counter, predecessor.getMyname(), predecessor.getmyPort());
								System.out.println("forwarded");
								to_remove.add(key);
								try {
						            // thread to sleep for 1000 milliseconds
						            sleep(1000);
						         } catch (Exception e) {
						            System.out.println(e);
						         }
							}
						} catch (NoSuchAlgorithmException e1) {
							e1.printStackTrace();
						}
						
					}
	        		for (String s : to_remove){
	        			files.remove(s);
	        		}
	        	}
	        	else {
    				forward_to(message_to_handle+"\n",replica_counter, successor.getMyname(), successor.getmyPort());
	        	}
	        }
		
	        try {
	        	serverSocket.close();
	        } catch (IOException e) {
	        	e.printStackTrace();
	        }
		}
	}

	// used to sort nodes after every join or depart in main 
	@Override
	public int compareTo(Node nd) {
		int compareId = (int) nd.getmyId();
		//ascending order
		return (int) (this.getmyId() - compareId);
		
	}
	
	public void forward_to(String message,int replicas, String hostname, int port){
		String message_final = message;
		String []message_with_replicas = message.split("-");
		if((!message_with_replicas[0].equals("ANSWER")) && (message_with_replicas.length >=4)){
			message_final = message_with_replicas[0]+"-"+replicas+"-"+message_with_replicas[2]+"-"+message_with_replicas[3];
		}
		Socket socket = null;
		try {
			socket = new Socket(hostname, port);
		} 
		catch (UnknownHostException e) {
		     System.out.println("Unknown host");
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
			System.exit(1);
		}
        OutputStreamWriter osw = new OutputStreamWriter(os);
        BufferedWriter bw = new BufferedWriter(osw);
        try {
			bw.write(message_final);
            bw.flush();
		} catch (IOException e) {
			System.out.println("Node "+myid+": Couldn't write to BufferWriter");
			e.printStackTrace();
			System.exit(1);
		}
	}
}
