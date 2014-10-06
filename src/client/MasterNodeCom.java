package client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class MasterNodeCom {

	public enum Request {
		LS, RMDIR, MKDIR, DELETE, UPLOAD, DOWNLOAD
	}

	/**
	 * Send JSON request to master node
	 * @param request
	 * @param s
	 * @param out
	 * @param path
	 * @param filesize
	 * @return
	 */
	public static boolean sendRequest(Request request, Socket s,
			DataOutputStream out, String path, long filesize) {

		if (out == null) {
			System.err.println("No open output stream with master node.");
			return false;
		}

		switch (request) {
		case LS:
			lsRequest(s, out);
			break;
		case RMDIR:
			removeDir(s, out, path);
			break;
		case MKDIR:
			makeDir(s, out, path);
			break;
		case DELETE:
			deleteFile(s, out, path);
			break;
		case UPLOAD:
			uploadFile(s, out, path, filesize);
			break;
		case DOWNLOAD:
			downloadFile(s, out, path);
			break;
		}

		return true;
	}

	/**
	 * Wait for JSON response from master node
	 * @param in
	 * @return
	 */
	public static JSONObject getResponse(DataInputStream in) {

		if (in == null) {
			System.err.println("No open input stream with master node.");
			return null;
		}

		String s = "";
		try {
			s = in.readUTF();
		} catch (IOException e) {
			e.printStackTrace();
		}

		JSONObject json = (JSONObject) JSONValue.parse(s);

		return json;

	}

	@SuppressWarnings("unchecked")
	private static void uploadFile(Socket s, DataOutputStream out,
			String filePath, long filesize) {
		try {
			JSONObject json = new JSONObject();
			json.put("upload", 1);
			json.put("file", filePath);
			json.put("size", filesize);

			out.writeBytes(json.toString() + '\n');
		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

	@SuppressWarnings("unchecked")
	private static void downloadFile(Socket s, DataOutputStream out,
			String filePath) {
		try {
			JSONObject json = new JSONObject();
			json.put("download", 1);
			json.put("file", filePath);

			out.writeBytes(json.toString() + '\n');
		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

	@SuppressWarnings("unchecked")
	private static void deleteFile(Socket s, DataOutputStream out,
			String filePath) {
		try {
			JSONObject json = new JSONObject();
			json.put("delete", 1);
			json.put("file", filePath);

			out.writeBytes(json.toString() + '\n');
		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

	@SuppressWarnings("unchecked")
	private static void makeDir(Socket s, DataOutputStream out, String dir) {
		try {
			JSONObject json = new JSONObject();
			json.put("make", 1);
			json.put("dir", dir);

			out.writeBytes(json.toString() + '\n');
		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

	@SuppressWarnings("unchecked")
	private static void removeDir(Socket s, DataOutputStream out, String dir) {

		try {
			JSONObject json = new JSONObject();
			json.put("remove", 1);
			json.put("dir", dir);

			out.writeBytes(json.toString() + '\n');
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	private static void lsRequest(Socket s, DataOutputStream out) {

		try {
			JSONObject json = new JSONObject();
			json.put("ls", 1);

			out.writeBytes(json.toString() + '\n');
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}
