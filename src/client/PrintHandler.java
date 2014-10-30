package client;

import java.io.FileNotFoundException;
import java.io.PrintStream;

public class PrintHandler extends PrintStream {

	public PrintHandler(String file) throws FileNotFoundException {
		super(file);

	}

	@Override
	public void println(double d) {
		checkCapacity();
		//		HTTPServer.HandleInfo.sb.append(d + "\n");
		super.println(d);
	}

	@Override
	public void println(int i) {
		checkCapacity();
		//		HTTPServer.HandleInfo.sb.append(i + "\n");
		super.println(i);
	}

	@Override
	public void println() {
		checkCapacity();
		//		HTTPServer.HandleInfo.sb.append("\n");

		super.println();
	}

	@Override
	public void println(String message) {
		checkCapacity();
		//		HTTPServer.HandleInfo.sb.append(message + "\n");
		super.println(message);
	}

	private void checkCapacity() {
		//		if (HTTPServer.HandleInfo.sb.length() > 40000) {
		//			HTTPServer.HandleInfo.sb = new StringBuilder();
		//			System.out.println("(New StringBuilder)");
		//		}
	}

}
