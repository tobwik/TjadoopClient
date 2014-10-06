package client;

import java.io.*;

public class Packet {
	// @formatter:off
	// Message parameter types
	public static final int INT_LOWER 		= 0x00;
	public static final int CHUNK_SIZE 		= 0x01;
	public static final int INT_UPPER 		= 0x4f;

	public static final int STR_LOWER 		= 0x50;
	public static final int DOWNLOAD 		= 0x51;
	public static final int UPLOAD 			= 0x52;
	public static final int SLAVE_ADDR 		= 0x53;
	public static final int DIRECTORIES 	= 0x54;
	public static final int MKDIR 			= 0x55;
	public static final int RMDIR 			= 0x56;
	public static final int LS 				= 0x57;
	public static final int STR_UPPER 		= 0x9f;

	public static final int BYTE_LOWER 		= 0x31;
	//	public static final int BYTE_
	public static final int BYTE_UPPER 		= 0x40;

	public static final int LONG_LOWER 		= 0x41;
	public static final int FILE_SIZE 		= 0x43;
	public static final int LONG_UPPER 		= 0x50;

	// Packets without parameter 0xa0 - 0xff
	public static final int CMD_OK 			= 0xb0;
	public static final int CMD_NOK 		= 0xb1;
	// @formatter:on

	private final int packetType;
	private int intData;
	private String strData;
	private byte[] byteData;
	private long longData;

	/**
	 * Constructor for int packets.
	 */
	public Packet(int packetType, int intData) {
		if (!(packetType >= INT_LOWER && packetType <= INT_UPPER))
			System.err.printf("Packet type does not match input! (0x%2X)\n",
					packetType);
		this.packetType = packetType;
		this.intData = intData;
	}

	/**
	 * Constructor for string packets.
	 */
	public Packet(int packetType, String strData) {
		if (!(packetType >= STR_LOWER && packetType <= STR_UPPER))
			System.err.printf("Packet type does not match input! (0x%2X)\n",
					packetType);
		this.packetType = packetType;
		this.strData = strData;
	}

	/**
	 * Constructor for string packets.
	 */
	public Packet(int packetType, long longData) {
		if (!(packetType >= LONG_LOWER && packetType <= LONG_UPPER))
			System.err.printf("Packet type does not match input! (0x%2X)\n",
					packetType);
		this.packetType = packetType;
		this.longData = longData;
	}

	/**
	 * Constructor for byte packets
	 * @param packetType
	 * @param byteData
	 */
	public Packet(int packetType, byte[] bd) {
		if (!(packetType >= STR_LOWER && packetType <= STR_UPPER))
			System.err.printf("Packet type does not match input! (0x%2X)\n",
					packetType);
		this.packetType = packetType;
		byteData = bd;
	}

	/**
	 * Constructor for packets without parameter.
	 */
	public Packet(int packetType) {
		this.packetType = packetType;
	}

	/**
	 * Returns the packet type as an int in the range 0x00 - 0xff
	 */
	public int getPacketType() {
		return packetType;
	}

	/**
	 * Returns the data int.
	 */
	public int getIntData() {
		if (intData == Integer.MIN_VALUE)
			System.err.println("An null int is returned, possible error!");
		return intData;
	}

	/**
	 * Returns the data string.
	 */
	public String getStrData() {
		if (strData == null)
			System.err.println("An null string is returned, possible error!");
		return strData;
	}

	/**
	 * Returns the data string.
	 */
	public long getLongData() {
		if (longData == Long.MIN_VALUE)
			System.err.println("An null long is returned, possible error!");
		return longData;
	}

	/**
	 * Returns the data string.
	 */
	public byte[] getByteData() {
		if (byteData == null)
			System.err
					.println("An null byte array is returned, possible error!");
		return byteData;
	}

	/**
	 * Writes ATMPacket to a stream.
	 */
	public void writeOut(DataOutputStream out) throws IOException {
		out.write((byte) this.packetType & 0xff);
		if ((this.packetType >= INT_LOWER) && (packetType <= INT_UPPER))
			out.writeInt(this.intData);
		else if (packetType >= STR_LOWER && packetType <= STR_UPPER)
			out.writeUTF(this.strData);
	}

	public static Packet readBytes(DataInputStream in, int chunksize)
			throws IOException {
		byte[] byteData = new byte[chunksize];

		// Read messagetype
		int packetType = -1;
		do {
			packetType = in.read();
		} while (packetType == -1);

		if (packetType >= BYTE_LOWER && packetType <= BYTE_UPPER) {
			//			strData = in.readByte();
			in.read(byteData);
			// Return new packet
			return new Packet(packetType, byteData);
		} else {
			// Return new packet
			return new Packet(packetType);
		}
	}

	/**
	 * Reads from a stream and returns a ATMPacket.
	 */
	public static Packet readIn(DataInputStream in) throws IOException,
			ClassNotFoundException {
		// Read messagetype
		int packetType = -1;
		do {
			packetType = in.read();
		} while (packetType == -1);

		int intData = 0;
		String strData = "";
		long longData = 0;

		// The range specifies parameter type
		if (packetType >= INT_LOWER && packetType <= INT_UPPER) {
			intData = in.readInt();
			// Return new packet
			return new Packet(packetType, intData);
		} else if (packetType >= STR_LOWER && packetType <= STR_UPPER) {
			strData = in.readUTF();
			// Return new packet
			return new Packet(packetType, strData);
		} else if (packetType >= LONG_LOWER && packetType <= LONG_UPPER) {
			longData = in.readLong();
			// Return new packet
			return new Packet(packetType, longData);
		} else {
			// Return new packet
			return new Packet(packetType);
		}

	}
}
