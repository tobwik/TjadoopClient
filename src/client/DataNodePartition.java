package client;

public class DataNodePartition {

	public final long[] starts;

	public final long[] ends;

	public final String ip;

	public DataNodePartition(long[] starts, long[] ends, String ip) {
		this.starts = new long[starts.length];
		System.arraycopy(starts, 0, this.starts, 0, starts.length);
		this.ends = new long[ends.length];
		System.arraycopy(ends, 0, this.ends, 0, ends.length);
		this.ip = ip;
	}
}
