package client;

import java.io.*;
import java.net.*;
import java.util.Scanner;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import client.PrintHandler;

/**
 * Tjadoop client
 *  upload big.mkv 100000000
 *  130.229.155.38:1337
 */

public class Client {

	public enum Request {
		LS, RMDIR, MKDIR, DELETE, UPLOAD, DOWNLOAD
	}

	private static int masterPort = 1337;
	public static int slavePort = 49384;
	private static String slave1Address = "";

	private static Socket socket = null;
	public static DataOutputStream out = null;
	private static DataInputStream in = null;
	private final String master;

	public Client(String address) throws NumberFormatException, Exception {
		master = address;
		//openConnection(address, masterPort);

		Scanner scanner = new Scanner(System.in);
		boolean doRun = true;
		while (doRun) {
			System.out.print(">>");
			String input = scanner.nextLine();
			String[] sArray = input.split(" ");

			// TODO byt tillbaka till JRE 7 och använd switch, för i helskotta
			//switch (sArray[0]) {
			if (sArray[0].equals("ls")) {
				System.out.println("Listing directory");
				String dir = "";
				if (sArray.length > 1)
					dir = sArray[1];
				listDirectory(dir);
			} else if (sArray[0].equals("rmdir")) {
				if (!validateCommand(sArray)) {
					System.out.println("Usage: rmdir <dir>");
					break;
				}
				removeDirectory(sArray[1]);
				System.out.println("Removing directory");
			} else if (sArray[0].equals("mkdir")) {
				if (!validateCommand(sArray)) {
					System.out.println("Usage: mkdir <dir>");
					break;
				}
				makeDirectory(sArray[1]);
				System.out.println("Creating directory");
			} else if (sArray[0].equals("up")) {
				if (!validateCommand(sArray)) {
					System.out.println("Usage: upload <dir> <duplication>");
					break;
				}
				uploadFile(sArray);
			} else if (sArray[0].equals("dl")) {
				if (!validateCommand(sArray)) {
					System.out.println("Usage: download <dir>");
					break;
				}
				downloadFile(sArray);
				System.out.println("Download file");
			} else if (sArray[0].equals("rm")) {
				if (!validateCommand(sArray)) {
					System.out.println("Usage: delete <dir/file>");
					break;
				}
				deleteFile(sArray[1]);
				System.out.println("Delete file");
			} else
				System.out.println("Not a valid command");
			//			default:
		}
		//}
		scanner.close();
		socket.close();
	}

	public static void openConnection(String address, int port)
			throws IOException {
		closeConnection();

		if (port == -1) {
			System.err.println("Failed to open connection, no valid port.");
			return;
		}

		try {
			socket = new Socket(address, port);
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

	public static void closeConnection() throws IOException {
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

		/*if (input[0].equals("upload")) {
			if (input.length >= 3 && input[input.length - 1].matches("[0-9]+"))
				res = true;
			else {
				System.out.println("Specify duplication amount");
				return false;
			}
		}*/

		return res;

	}

	private DataNodePartition[] getPartitionMapFromMaster(JSONArray resArray,
			boolean isUpload) {
		DataNodePartition[] dataNodePartitions = new DataNodePartition[resArray
				.size()];

		for (int i = 0; i < resArray.size(); ++i) {
			JSONObject jsonObject = (JSONObject) resArray.get(i);
			String ip = "";
			if (i == 0 && isUpload) {
				slave1Address = (String) jsonObject.get("ip");
				ip = (String) jsonObject.get("ip");
			} else {
				ip = (String) jsonObject.get("ip");
			}
			JSONArray partitions = (JSONArray) jsonObject.get("partitions");
			long[] starts = new long[partitions.size()];
			long[] ends = new long[partitions.size()];

			System.out.println("Partitions array json:");
			System.out.println(partitions.toString());

			for (int j = 0; j < partitions.size(); ++j) {
				JSONObject partition = (JSONObject) partitions.get(j);
				System.out.println("Json object: " + partition.toString());
				starts[j] = (Long) partition.get("start");
				ends[j] = (Long) partition.get("end");
			}

			System.out.println("starts");
			for (int h = 0; h < starts.length; h++) {
				System.out.println(starts[h]);
			}
			System.out.println("Ends");
			for (int h = 0; h < ends.length; h++) {
				System.out.println(ends[h]);
			}

			dataNodePartitions[i] = new DataNodePartition(starts, ends, ip);
		}

		return dataNodePartitions;

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

		/* Communication with master */

		openConnection(master, masterPort);

		MasterNodeCom.sendRequest(Request.DOWNLOAD, socket, out, fileName, 0);

		JSONObject json = MasterNodeCom.getResponse(in);

		boolean success = (Boolean) json.get("success");
		if (!success) {
			System.out
					.println("Failed to download file, master does not approve");
			return null;
		}

		JSONArray resArray = (JSONArray) json.get("nodes");
		System.out.println(resArray.toString());

		DataNodePartition[] dataNodePartitions = getPartitionMapFromMaster(
				resArray, false);

		//		String slaveAddr = (String) json.get("ip");

		byte type = 1;
		System.out.println("Downloading file " + fileName);
		String filePath = "/" + fileName;
		int fileHash = filePath.hashCode();

		for (int p = 0; p < dataNodePartitions.length; p++) {
			System.out.println("Geting partition from "
					+ dataNodePartitions[p].ip);

			//out.writeInt(starts.length);
			//System.out.println("Num nodes: " + starts.length);

			long[] starts = dataNodePartitions[p].starts;
			long[] ends = dataNodePartitions[p].ends;

			for (int k = 0; k < starts.length; k++) {

				openConnection(dataNodePartitions[p].ip, slavePort);

				System.out.println("Available inputstream?: " + in.available());

				System.out.println("Sending creator data");
				out.write(type);
				out.writeInt(fileHash);

				System.out.println("Type: " + type);
				System.out.println("File hash: " + fileHash);

				//out.writeUTF(dataNodePartitions[p].ip);
				out.writeLong(starts[k]);
				out.writeLong(ends[k]);

				/* Communication with slave */
				if (!DataNodeCom.download(in, fileName, dataNodePartitions[p],
						starts[k], ends[k])) {
					System.err.println("Failed to download.");
					return null;
				}
			}

		}

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

		System.out.println("Connecting to master");
		openConnection(master, masterPort);
		System.out.println("Connection successful");

		System.out.println("Uploading file...");

		String file = parseFilePath(input);

		/* - Communication with master - */
		long filesize = new File(file).length();

		//		System.out.println(filesize);
		//System.out.println(out);

		if (!MasterNodeCom.sendRequest(Request.UPLOAD, socket, out, file,
				filesize))
			return false;

		/* Master response */
		JSONObject json = MasterNodeCom.getResponse(in);

		if (!(Boolean) json.get("success")) {
			System.out
					.println("Failed to upload file, master does not approve");
			return false;
		}

		//Gson gson = new Gson();
		//String id = (String) json.get("id");
		JSONArray resArray = (JSONArray) json.get("nodes");
		System.out.println(resArray.toString());

		DataNodePartition[] dataNodePartitions = getPartitionMapFromMaster(
				resArray, true);

		/* Communication with slave */
		//closeConnection();

		//System.out.println("Slave address: " + slave1Address);

		if (!DataNodeCom.upload(file, filesize, dataNodePartitions)) {
			System.err.println("Failed to upload.");
			return false;
		}

		System.out.println("Uploaded file.");

		//TODO send file size and receive partition size from server
		// then split file
		// then send files to specified location (received ip from server)
		//för att inte ta upp för mycket RAM när jag splittar så bör jag 
		return true;
	}

	private boolean makeDirectory(String dir) throws IOException,
			ClassNotFoundException {
		openConnection(master, masterPort);

		/* form: /tobbe/skumpa , inte /tobbe/skumpa/ */// TODO fixa parser för detta

		if (!MasterNodeCom.sendRequest(Request.MKDIR, socket, out, dir, 0))
			return false;
		JSONObject json = MasterNodeCom.getResponse(in);

		if (!(Boolean) json.get("success"))
			return false;

		return true;
	}

	private boolean removeDirectory(String dir) throws IOException,
			ClassNotFoundException {
		openConnection(master, masterPort);

		if (!MasterNodeCom.sendRequest(Request.RMDIR, socket, out, dir, 0))
			return false;
		JSONObject json = MasterNodeCom.getResponse(in);

		if (!(Boolean) json.get("success")) {
			System.err.println("Failed to send to master");
			return false;
		}

		// TODD vänta på en till ACK, för att directory har blivit borttaget
		return true;
	}

	private String listDirectory(String dir) throws IOException,
			ClassNotFoundException {

		/* Master communication */

		openConnection(master, masterPort);

		MasterNodeCom.sendRequest(Request.LS, socket, out, dir, 0);

		JSONObject json = MasterNodeCom.getResponse(in);

		if (!(Boolean) json.get("success")) {
			System.out.println("Directory not found");
			return null;
		}
		JSONArray ret = (JSONArray) json.get("content");

		System.out.println(ret.toString().replace("\\", ""));
		return ret.toString().replace("\\", "");

	}

	private boolean deleteFile(String file) throws IOException {
		openConnection(master, masterPort);

		if (!MasterNodeCom.sendRequest(Request.DELETE, socket, out, file, 0))
			return false;
		JSONObject json = MasterNodeCom.getResponse(in);

		System.out.println(json.toString());

		if (!json.get("cmd").equals("ack-rmfile")) {
			System.err.println("Master does not approve");
			return false;
		}

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

		//		System.setOut(new PrintHandler("loggis.txt"));
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

}
