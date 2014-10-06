package client;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import client.MasterNodeCom.Request;

/**
 * Tjadoop client
 *  upload big.mkv 100000000
 */

public class Client {

	private static int connectionPort = 8989;
	Socket socket = null;
	private DataOutputStream out = null;
	private DataInputStream in = null;
	private final String master;

	public Client(String address) throws NumberFormatException, Exception {
		master = address;
		//		openConnection(address);

		Scanner scanner = new Scanner(System.in);
		boolean doRun = true;
		while (doRun) {
			System.out.print(">>");
			String input = scanner.nextLine();
			String[] sArray = input.split(" ");

			switch (sArray[0]) {
			case ("ls"):
				System.out.println("Listing directory");
				String dir = "";
				if (sArray.length > 1)
					dir = sArray[1];
				listDirectory(dir);
				break;
			case ("rmdir"):
				if (!validateCommand(sArray)) {
					System.out.println("Usage: rmdir <dir>");
					break;
				}
				removeDirectory(sArray[1]);
				System.out.println("Removing directory");
				break;
			case ("mkdir"):
				if (!validateCommand(sArray)) {
					System.out.println("Usage: mkdir <dir>");
					break;
				}
				makeDirectory(sArray[1]);
				System.out.println("Creating directory");
				break;
			case ("upload"):
				if (!validateCommand(sArray)) {
					System.out.println("Usage: upload <dir> <duplication>");
					break;
				}
				uploadFile(sArray);
				break;
			case ("download"):
				if (!validateCommand(sArray)) {
					System.out.println("Usage: download <dir>");
					break;
				}
				downloadFile(sArray);
				System.out.println("Download file");
				break;
			case ("delete"):
				if (!validateCommand(sArray)) {
					System.out.println("Usage: delete <dir/file>");
					break;
				}
				deleteFile(sArray[1]);
				System.out.println("Delete file");
				break;
			default:
				System.out.println("Not a valid command");
			}
		}
		scanner.close();
		socket.close();
	}

	private void openConnection(String address) throws IOException {
		closeConnection();

		try {
			socket = new Socket(address, connectionPort);
			out = new DataOutputStream(socket.getOutputStream());
			in = new DataInputStream(socket.getInputStream());
		} catch (UnknownHostException e) {
			System.err.println("Unknown host: " + address);
			System.exit(1);
		} catch (IOException e) {
			System.err.println("Couldn't open connection to " + address);
			System.exit(1);
		}
	}

	private void closeConnection() throws IOException {
		if (socket != null)
			socket.close();
		if (out != null)
			out.close();
		if (in != null)
			in.close();
	}

	private String parseFilePath(String[] input) {
		if (input.length > 3) {
			StringBuilder sb = new StringBuilder();
			if (input[1].startsWith("\"")
					&& input[input.length - 2].endsWith("\"")) {
				for (int i = 1; i < input.length - 1; i++) {
					sb.append(input[i] + " ");
				}
				sb.replace(0, 1, "");
				sb.replace(sb.length() - 2, sb.length() - 1, "");
			} else
				return "";
			return sb.toString().trim();
		} else {
			return input[1].trim();
		}
	}

	private boolean validateCommand(String[] input) {
		boolean res = false;

		if (input.length < 2) {
			System.out.println("Incorrect input length");
			return false;
		} else
			res = true;

		if (input[0].equals("upload")) {
			if (input.length >= 3 && input[input.length - 1].matches("[0-9]+"))
				res = true;
			else {
				System.out.println("Specify duplication amount");
				return false;
			}
		}

		return res;

	}

	/**
	 * Download file in partitions from slaves
	 * @param input
	 * @return
	 * @throws ClassNotFoundException
	 * @throws IOException
	 */
	private File downloadFile(String[] input) throws ClassNotFoundException,
			IOException {
		String fileName = input[1];
		//		Says what it wants to download to master. (file path)
		//		Master can return "false" if it doesn't exists, obviously.
		//		Otherwise, returns a list with data nodes List[id, bytesize].
		//
		//		Master redirects to nodes that contains the blocks to form the file.
		//
		//		Data nodes send blocks to client.
		//
		//		When all blocks have been received, client patches them together to a file.
		//
		//		Done!

		/* Communication with master */

		MasterNodeCom.sendRequest(Request.DOWNLOAD, socket, out, fileName, 0);

		JSONObject json = MasterNodeCom.getResponse(in);

		boolean success = (boolean) json.get("ok");
		if (!success) {
			System.out
					.println("Failed to download file, master does not approve");
			return null;
		}
		String slaveAddr = (String) json.get("id");
		long fileSize = (long) json.get("size");
		Map<Integer, ByteTuple> blockMap = new HashMap<Integer, ByteTuple>();

		JSONArray jsonArray = (JSONArray) json.get("map");

		for (int i = 0; i < jsonArray.size(); i++) {
			JSONObject o = (JSONObject) jsonArray.get(i);
			blockMap.put((Integer) o.get("key"),
					new ByteTuple((long) o.get("start"), (long) o.get("end")));
		}

		/* Communication with slave */

		//		openConnection(slaveAddr);

		//		boolean notDone = true;
		//		long downloadedBytes = 0;
		//		int numberOfFullChunks = (int) Math.floor((filesize / chunksize));
		//		int rest = (int) (filesize - (numberOfFullChunks * chunksize));
		//		int i = 1;
		//		while (notDone) {
		//			Packet p;
		//			if (i == numberOfFullChunks) {
		//				p = Packet.readBytes(in, rest);
		//				notDone = false;
		//			} else
		//				p = Packet.readBytes(in, chunksize);
		//
		//			byte[] bytes = p.getByteData();
		//			//TODO gör något med bytsen, skriv till fil! Join från FileHandler
		//			downloadedBytes += bytes.length;
		//			i++;
		//		}
		//		System.out.println("Downloaded " + downloadedBytes + " bytes");
		//
		return null;
	}

	/**
	 * Upload file in partitions to slaves
	 * @param input
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException 
	 */
	private boolean uploadFile(String[] input) throws IOException,
			ClassNotFoundException {
		//		Says where it wants to upload to the master, name, total size
		//
		//		Receiver True/false, and if true, a Map[id, (byte start, byte end)]
		//		id is data node id. So the list contains information on where each block should be stored.
		//
		//		Get redirected to a data node, sends how much duplication (n st) you want.
		//
		//		//Master tells client how many blocks the data should be divided in.
		//
		//		Client divides the data into blocks.
		//
		//		Then the master redirects the client to n data nodes, and it should also tell the client which blocks it should upload on to the data node.
		//
		//		Start uploading.
		//
		//		Done!
		boolean test = true;
		System.out.println("Uploading file...");

		String file = parseFilePath(input);

		/* Communication with master */
		int chunksize = 0;
		String slaveAddr = "";
		long filesize = new File(file).length();

		if (!MasterNodeCom.sendRequest(Request.UPLOAD, socket, out, file,
				filesize))
			return false;

		//		if (!test) {
		//			openConnection(master);
		//
		//			new Packet(Packet.UPLOAD, file).writeOut(out);
		//
		//			int count = 3; //Wait for 4 packets from master
		//			while (count > 0) {
		//
		//				Packet p = (Packet.readIn(in));
		//				int packetType = p.getPacketType();
		//
		//				switch (packetType) {
		//				case (Packet.CMD_OK):
		//					count--;
		//					System.out.println("Upload starting...");
		//					break;
		//				case (Packet.CMD_NOK):
		//					//TODO
		//					System.out.println("File doesn't exist");
		//					count = -1;
		//					break;
		//				case (Packet.CHUNK_SIZE):
		//					//TODO
		//					chunksize = p.getIntData();
		//					System.out.println("Chunk size: " + chunksize);
		//					count--;
		//					break;
		//				case (Packet.SLAVE_ADDR):
		//					//TODO
		//					slaveAddr = p.getStrData();
		//					System.out.println("Slave address: " + slaveAddr);
		//					count--;
		//					break;
		//				}
		//			}
		//
		//			if (count == -1)
		//				return false;
		//
		//			/* Communication with slave */
		//
		//			openConnection(slaveAddr);
		//		}

		ArrayList<byte[]> bytes = null;

		long readBytes = 0;
		int i = 0;
		chunksize = Integer.parseInt(input[input.length - 1]); //TODO denna är bara för testning, ta bort
		while (readBytes < filesize) {

			bytes = FileHandler.getBytes(file, 1, readBytes, chunksize);
			for (byte[] ba : bytes) {
				readBytes += ba.length;
				//read bytes here (send somewhere for example)
			}
			System.out.println("Created " + bytes.size()
					+ " chunks. Chunk size "
					+ bytes.get(bytes.size() - 1).length + ".");
			FileHandler.createPartitionFile("apa", i, bytes.get(0));
			i++;
		}

		//TODO send file size and receive partition size from server
		// then split file
		// then send files to specified location (received ip from server)
		//för att inte ta upp för mycket RAM när jag splittar så bör jag 
		System.out.println("Uploaded file.");
		return true;
	}

	private boolean makeDirectory(String dir) throws IOException,
			ClassNotFoundException {
		openConnection(master);

		if (!MasterNodeCom.sendRequest(Request.MKDIR, socket, out, dir, 0))
			return false;
		JSONObject json = MasterNodeCom.getResponse(in);

		if (!(boolean) json.get("ok"))
			return false;

		return true;
	}

	private boolean removeDirectory(String dir) throws IOException,
			ClassNotFoundException {
		openConnection(master);

		if (!MasterNodeCom.sendRequest(Request.RMDIR, socket, out, dir, 0))
			return false;
		JSONObject json = MasterNodeCom.getResponse(in);

		if (!(boolean) json.get("ok"))
			return false;

		return true;
	}

	private String listDirectory(String dir) throws IOException,
			ClassNotFoundException {
		openConnection(master);
		String ret = "";
		//		new Packet(Packet.LS, dir).writeOut(out);

		/* Master communication */
		if (!MasterNodeCom.sendRequest(Request.LS, socket, out, null, 0))
			return null;

		JSONObject json = MasterNodeCom.getResponse(in);

		boolean ok = (boolean) json.get("ok");
		if (!ok) {
			System.out.println("Directory not found");
			return null;
		}
		ret = (String) json.get("dir");
		return ret;

	}

	private boolean deleteFile(String file) throws IOException {
		openConnection(master);

		if (!MasterNodeCom.sendRequest(Request.DELETE, socket, out, file, 0))
			return false;
		JSONObject json = MasterNodeCom.getResponse(in);

		if (!(boolean) json.get("ok"))
			return false;

		return true;

	}

	/**
	 * main method
	 * 
	 * @param args
	 *            The ip adress of server
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws IOException,
			ClassNotFoundException {
		try {
			new Client(args[0]);
		} catch (ArrayIndexOutOfBoundsException e) {
			System.err.println("Missing argument ip-adress");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException n) {
			System.err.println("Number format");
			System.exit(1);
		} catch (Exception e) {
			System.err.println("Caught unknown exception: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}

	}

	private class ByteTuple {
		public long start;
		public long end;

		public ByteTuple(long s, long e) {
			start = s;
			end = e;
		}
	}

}
