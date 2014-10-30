package client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

public class DataNodeCom {

	public static boolean upload(DataOutputStream out, String file,
			long filesize, DataNodePartition[] dataNodePartitions)
			throws FileNotFoundException, IOException {

		ArrayList<byte[]> sendBytes = null;
		int sequenceNumber = 0;
		long dataBytes = 0;
		byte type = 0;
		System.out.println("Uploading file " + file);
		String filePath = "/" + file;
		int fileHash = filePath.hashCode();
		//int numNodes = dataNodePartitions.length; // kanske borde vara en short TODO

		for (int p = 0; p < dataNodePartitions.length; p++) {
			Client.openConnection(dataNodePartitions[p].ip, Client.slavePort);

			// Meta data/Creator data
			System.out.println("Sending creator data");
			Client.out.write(type);
			Client.out.writeInt(sequenceNumber); // sequence number
			Client.out.writeInt(fileHash);

			System.out.println("Sending ips, starts and ends");
			System.out.println("Type: " + type);
			System.out.println("File hash: " + fileHash);
			System.out.println("Partitions: " + dataNodePartitions.length);
			System.out.println("Sending partition");

			long[] starts = dataNodePartitions[p].starts;
			long[] ends = dataNodePartitions[p].ends;

			//			System.out.println("starts");
			//			for (int i = 0; i < starts.length; i++) {
			//				System.out.println(starts[i]);
			//			}
			//			System.out.println("Ends");
			//			for (int i = 0; i < ends.length; i++) {
			//				System.out.println(ends[i]);
			//			}

			Client.out.writeInt(starts.length);
			System.out.println("Num nodes: " + starts.length);

			for (int k = 0; k < starts.length; k++) {
				Client.out.writeUTF(dataNodePartitions[p].ip);
				System.out
						.println("Sending to ip: " + dataNodePartitions[p].ip);
				Client.out.writeLong(starts[k]);
				System.out.println("Sending start: " + starts[k]);
				Client.out.writeLong(ends[k]);
				System.out.println("Sending end: " + ends[k]);
			}

			System.out.println("Sending file size");
			System.out.println("File size: " + filesize);

			Client.out.writeLong(filesize);

			// Start sending the data
			for (int k = 0; k < starts.length; k++) {

				// TODO Detta bör göras bättre, stora filer med få partitioner kommer langa memory probs
				sendBytes = FileHandler.getBytes(file, 1, starts[k], ends[k]
						- starts[k] + 1);

				for (byte[] b : sendBytes) {
					//					Client.out.write(b);
					dataBytes += b.length;
				}

			}
		}

		if (dataBytes == filesize * dataNodePartitions.length) {
			System.out.println("Upload complete");
			return true;
		}

		System.out.println("Upload failed, dataBytes != filesize");
		System.out.println("Data sent: " + dataBytes);
		return false;

	}

	public static boolean download(DataInputStream in, String file,
			DataNodePartition dataNodePartitions, long start, long end)
			throws FileNotFoundException, IOException {

		long dataBytes = 0;
		long filesize = 0;

		// TODO fixa timeout timer om jag inte får respons på ett tag
		System.out.println("Downloading and saving...");
		System.out.println("start: " + start);
		System.out.println("end: " + end);
		while (true) {

			byte[] receivedBytes = new byte[(int) (end - start) + 1];

			//dataBytes += in.read(receivedBytes);

			System.out.println("Starting to read " + receivedBytes.length
					+ " bytes");
			int count = 0;

			for (int i = 0; i < receivedBytes.length; i++) {
				count++;
				receivedBytes[i] = in.readByte();

				//				System.out.println("Received byte: " + i);

			}

			if (start == 0) {
				System.out.println("Creating file!");
				System.out.println("Byte count: " + count);
				FileHandler.createFile("downloaded_" + file, receivedBytes);
			} else {
				System.out.println("Joining to file: " + receivedBytes);
				System.out.println("Byte count: " + count);
				FileHandler.join("downloaded_" + file, receivedBytes);
			}

			// TODO ta bort, ändå inte relevant längre
			if (dataBytes == filesize) {
				System.out.println("Download complete");
				return true;
			} else if (dataBytes > filesize) {
				System.err
						.println("Received more bytes than file size, woot woot");
				return false;
			}

		}

	}
}
