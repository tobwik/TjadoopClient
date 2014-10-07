package client;

import java.io.*;
import java.util.ArrayList;

public class FileHandler {

	public static long chunkSize = 1000;

	/**
	 * 	Split the file specified by filename into pieces, each of size
	 * chunkSize except for the last one, which may be smaller.
	 * WARNING: Uses a lot of RAM
	 * 
	 * @param filename
	 * @param chunkS
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static ArrayList<byte[]> split(String filename, long chunkS)
			throws FileNotFoundException, IOException {
		chunkSize = chunkS;
		BufferedInputStream in = new BufferedInputStream(new FileInputStream(
				filename));
		File f = new File(filename);
		long fileSize = f.length();
		ArrayList<byte[]> res = new ArrayList<byte[]>();

		int subfile;
		for (subfile = 0; subfile < fileSize / chunkSize; subfile++) {

			byte[] bytes = new byte[(int) chunkSize];
			for (int currentByte = 0; currentByte < chunkSize; currentByte++) {
				int data = in.read();
				bytes[currentByte] = (byte) data;
			}
			res.add(bytes);

		}

		// loop for the last chunk (which may be smaller than the chunk size)
		if (fileSize != chunkSize * (subfile - 1)) {

			int b;
			byte[] bytes = new byte[(int) (fileSize - (chunkSize * subfile))];
			int i = 0;
			while ((b = in.read()) != -1) {
				bytes[i] = (byte) b;
				i++;
			}
			res.add(bytes);
		}

		in.close();
		return res;
	}

	/**
	 * Split file into chunks and write them to disk.
	 * @param filename
	 * @param chunkS
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static void splitToFiles(String filename, long chunkS)
			throws FileNotFoundException, IOException {
		chunkSize = chunkS;
		BufferedInputStream in = new BufferedInputStream(new FileInputStream(
				filename));
		File f = new File(filename);
		long fileSize = f.length();

		// loop for each full chunk
		int subfile;
		for (subfile = 0; subfile < fileSize / chunkSize; subfile++) {
			BufferedOutputStream out = new BufferedOutputStream(
					new FileOutputStream(filename + "." + subfile));

			for (int currentByte = 0; currentByte < chunkSize; currentByte++) {
				int data = in.read();
				out.write(data);
			}

			out.close();
		}

		// loop for the last chunk (which may be smaller than the chunk size)
		if (fileSize != chunkSize * (subfile - 1)) {
			BufferedOutputStream out = new BufferedOutputStream(
					new FileOutputStream(filename + "." + subfile));

			int b;

			while ((b = in.read()) != -1) {
				out.write(b);
			}
			out.close();
		}

		in.close();
	}

	/**
	 * Read bytes in chunks from file.
	 * This is the best method to spare both RAM and time.
	 * @param filename
	 * @param chunks
	 * @param offset
	 * @param chunksize
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static ArrayList<byte[]> getBytes(String filename, int chunks,
			long offset, long chunksize) throws FileNotFoundException,
			IOException {

		BufferedInputStream in = new BufferedInputStream(new FileInputStream(
				filename));
		File f = new File(filename);
		long fileSize = f.length();
		ArrayList<byte[]> res = new ArrayList<byte[]>();

		int subfile;
		in.skip(offset);
		long readBytes = offset;
		for (subfile = 0; subfile < chunks; subfile++) {

			byte[] bytes;
			if (chunksize <= fileSize - readBytes) {

				bytes = new byte[(int) chunksize];
				in.read(bytes, 0, (int) chunksize);
				res.add(bytes);
				readBytes += chunksize;
			} else { //only called once (last chunk)

				bytes = new byte[(int) (fileSize - readBytes)];
				in.read(bytes, 0, (int) (fileSize - readBytes));

				in.close();
				res.add(bytes);
				return res;

			}
		}

		in.close();
		return res;
	}

	/**
	 * Create a chunk file from byte array.
	 * @param fileName
	 * @param bytes
	 * @throws IOException
	 */
	public static void createPartitionFile(String fileName, int index,
			byte[] bytes) throws IOException {

		BufferedOutputStream out = new BufferedOutputStream(
				new FileOutputStream(fileName + "." + index));
		for (int i = 0; i < bytes.length; i++) {
			out.write(bytes[i]);
		}
		out.close();
	}

	/**
	 * Create a chunk file from byte array.
	 * @param fileName
	 * @param bytes
	 * @throws IOException
	 */
	public static void createFile(String fileName, byte[] bytes)
			throws IOException {

		BufferedOutputStream out = new BufferedOutputStream(
				new FileOutputStream(fileName));
		for (int i = 0; i < bytes.length; i++) {
			out.write(bytes[i]);
		}
		out.close();
	}

	/**
	 * Join files. (If RAM space gets overloaded, then create the partitioned files and join them instead)
	 * @param baseFilename
	 * @throws IOException
	 */
	public static void join(String baseFilename) throws IOException {
		int numberParts = getNumberParts(baseFilename);

		BufferedOutputStream out = new BufferedOutputStream(
				new FileOutputStream(baseFilename));
		for (int part = 0; part < numberParts; part++) {
			BufferedInputStream in = new BufferedInputStream(
					new FileInputStream(baseFilename + "." + part));

			int b;
			while ((b = in.read()) != -1)
				out.write(b);

			in.close();
		}
		out.close();
	}

	/**
	 * Join bytes from memory to create file. Faster but takes more RAM.
	 * @param filename
	 * @param bytes
	 * @throws IOException
	 */
	public static void join(String filename, ArrayList<byte[]> bytes)
			throws IOException {

		BufferedOutputStream out = new BufferedOutputStream(
				new FileOutputStream(filename));
		for (int part = 0; part < bytes.size(); part++) {
			byte[] byteArray = bytes.get(part);
			for (int b = 0; b < byteArray.length; b++) {
				out.write(byteArray[b]);
			}

		}
		out.close();
	}

	/**
	 * Join bytes from array to marge into existing file.
	 * @param filename
	 * @param bytes
	 * @throws IOException
	 */
	public static void join(String filename, byte[] bytes) throws IOException {

		BufferedOutputStream out = new BufferedOutputStream(
				new FileOutputStream(filename));
		for (int b = 0; b < bytes.length; b++) {
			out.write(bytes[b]);
		}

		out.close();
	}

	/**
	 * Find out how many chunks there are to the base filename
	 * @param baseFilename
	 * @return
	 * @throws IOException
	 */
	private static int getNumberParts(String baseFilename) throws IOException {
		// list all files in the same directory
		File directory = new File(baseFilename).getAbsoluteFile()
				.getParentFile();
		final String justFilename = new File(baseFilename).getName();
		String[] matchingFiles = directory.list(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.startsWith(justFilename)
						&& name.substring(justFilename.length()).matches(
								"^\\.\\d+$");
			}
		});
		return matchingFiles.length;
	}
}