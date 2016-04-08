package edu.cornell.library.integration.utilities;

import java.io.OutputStream;
import java.io.PrintStream;

public class TraceOutput extends PrintStream {

	public TraceOutput(OutputStream out) {
		super(out);
	}

	public void println(Object o) {
		StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
		super.println(o);
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
