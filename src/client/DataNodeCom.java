package client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

public class DataNodeCom {

	public static boolean upload(DataOutputStream out, String file,
			long filesize, int[] ids, long[] starts, long[] ends)
			throws FileNotFoundException, IOException {

		ArrayList<byte[]> sendBytes = null;

		int i = 0;
		long dataBytes = 0;
		byte type = 0;
		//chunksize = Integer.parseInt(input[input.length - 1]); //TODO denna är bara för testning, ta bort

		for (int k = 0; k < ids.length; k++) {

			sendBytes = FileHandler.getBytes(file, 1, starts[k], ends[k]
					- starts[k]);

			//Send load to data node

			out.write(type);
			out.write(ids[k]);
			out.writeLong(starts[k]);
			out.writeLong(ends[k]);

			for (byte[] b : sendBytes) {
				out.write(b);
				dataBytes += b.length;
			}

			if (k == ids.length - 1) {
				if (dataBytes == filesize) {
					System.out.println("Upload complete");
					return true;
				} else {
					System.err.println("Upload didn't finish!");
					return false;
				}
			}

		}

		return false;

	}

	public static boolean download(DataInputStream in)
			throws FileNotFoundException, IOException {

		ArrayList<byte[]> sendBytes = null;

		int i = 0;
		long dataBytes = 0;
		byte type = 0;
		//chunksize = Integer.parseInt(input[input.length - 1]); //TODO denna är bara för testning, ta bort

		while (true) {

			//			sendBytes = FileHandler.getBytes(file, 1, starts[k], ends[k]
			//					- starts[k]);

			//Send load to data node

			type = in.readByte();
			in.readLong(starts[k]);
			in.readLong(ends[k]);

			for (byte[] b : sendBytes) {
				out.write(b);
				dataBytes += b.length;
			}

			if (k == ids.length - 1) {
				if (dataBytes == filesize) {
					System.out.println("Upload complete");
					return true;
				} else {
					System.err.println("Upload didn't finish!");
					return false;
				}
			}

		}

		return false;

	}

}
