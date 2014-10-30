package client;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.Socket;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import client.Client.Request;

public class MasterNodeCom {

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
			lsRequest(s, out, path);
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
	 * @throws UnsupportedEncodingException 
	 */
	public static JSONObject getResponse(DataInputStream in)
			throws UnsupportedEncodingException {
		BufferedReader streamReader = new BufferedReader(new InputStreamReader(
				in, "UTF-8"));

		if (in == null) {
			System.err.println("No open input stream with master node.");
			return null;
		}

		//StringBuilder responseBuilder = new StringBuilder();
		StringBuilder sb = new StringBuilder();
		try {

			// read until num left brackets == right brackets
			int lb = 0;
			int rb = 0;
			do {
				byte c = in.readByte();
				sb.append((char) c);

				if (c == '{')
					lb++;
				if (c == '}')
					rb++;

			} while (lb != rb);

		} catch (IOException e) {
			e.printStackTrace();
		}
		JSONObject json = (JSONObject) JSONValue.parse(sb.toString());

		return json;

	}

	@SuppressWarnings("unchecked")
	private static void uploadFile(Socket s, DataOutputStream out,
			String filePath, long filesize) {
		try {
			JSONObject json = new JSONObject();
			json.put("cmd", "upload");
			json.put("path", "/" + filePath);
			json.put("size", filesize);

			out.writeBytes(json.toString().replace("\\", "") + '\n');
		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

	@SuppressWarnings("unchecked")
	private static void downloadFile(Socket s, DataOutputStream out,
			String filePath) {
		try {
			JSONObject json = new JSONObject();
			json.put("cmd", "download");
			json.put("path", "/" + filePath);

			out.writeBytes(json.toString().replace("\\", "") + '\n');
		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

	@SuppressWarnings("unchecked")
	private static void deleteFile(Socket s, DataOutputStream out,
			String filePath) {
		try {
			JSONObject json = new JSONObject();
			json.put("cmd", "rmfile");
			json.put("path", "/" + filePath);

			out.writeBytes(json.toString().replace("\\", "") + '\n');
		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

	@SuppressWarnings("unchecked")
	private static void makeDir(Socket s, DataOutputStream out, String dir) {
		try {
			JSONObject json = new JSONObject();
			json.put("cmd", "mkdir");
			json.put("path", dir);

			out.writeBytes(json.toString().replace("\\", "") + '\n');
		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

	@SuppressWarnings("unchecked")
	private static void removeDir(Socket s, DataOutputStream out, String dir) {

		try {
			JSONObject json = new JSONObject();
			json.put("cmd", "rmdir");
			json.put("path", dir);

			out.writeBytes(json.toString().replace("\\", "") + '\n');
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	private static void lsRequest(Socket s, DataOutputStream out, String path) {

		try {
			JSONObject json = new JSONObject();
			json.put("cmd", "ls");
			json.put("path", path);

			System.out.println(json.toString().replace("\\", "") + "\n");

			out.writeBytes(json.toString().replace("\\", "") + '\n');
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}
