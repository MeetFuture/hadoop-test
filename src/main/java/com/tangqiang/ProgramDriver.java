package com.tangqiang;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.TreeMap;

/**
 * 测试运行器
 *
 * @author tqiang
 * @email tqiang@grgbanking.com
 */
public class ProgramDriver {

	private Map<String, ProgramDescription> programs = new TreeMap<String, ProgramDescription>();

	private void printUsage(Map<String, ProgramDescription> programs) {
		System.out.println("	Usage : ");
		for (Map.Entry<String, ProgramDescription> item : programs.entrySet()) {
			System.out.println("  " + item.getKey() + ": " + item.getValue().getDescription());
		}
	}

	/**
	 * This is the method that adds the classed to the repository
	 * 
	 * @param name
	 *            The name of the string you want the class instance to be called with
	 * @param mainClass
	 *            The class that you want to add to the repository
	 * @param description
	 *            The description of the class
	 * @throws NoSuchMethodException
	 * @throws SecurityException
	 */
	public void addClass(String name, Class<?> mainClass, String description) throws Throwable {
		programs.put(name, new ProgramDescription(mainClass, description));
	}

	/**
	 * This is a driver for the example programs. It looks at the first command line argument and tries to find an example program with that name. If it is found, it calls the main method in that class with the rest of the command line arguments.
	 * 
	 * @param args
	 *            The argument from the user. args[0] is the command to run.
	 * @return -1 on error, 0 on success
	 * @throws NoSuchMethodException
	 * @throws SecurityException
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 * @throws Throwable
	 *             Anything thrown by the example program's main
	 */
	public int run(String[] args) throws Throwable {
		if (args.length == 0) {
			System.out.println("An example program must be given as the first argument.");
			printUsage(programs);
			return -1;
		}

		ProgramDescription pgm = programs.get(args[0]);
		if (pgm == null) {
			System.out.println("Unknown program '" + args[0] + "' chosen.");
			printUsage(programs);
			return -1;
		}

		String[] new_args = new String[args.length - 1];
		for (int i = 1; i < args.length; ++i) {
			new_args[i - 1] = args[i];
		}
		pgm.invoke(new_args);
		return 0;
	}
	
	
	private static class ProgramDescription {
		private Method main;
		private String description;
		static final Class<?>[] paramTypes = new Class<?>[] { String[].class };

		/**
		 * Create a description of an example program.
		 * @param mainClass
		 * @param description
		 * @throws Exception
		 */
		public ProgramDescription(Class<?> mainClass, String description) throws Exception {
			this.main = mainClass.getMethod("main", paramTypes);
			this.description = description;
		}

		/**
		 * Invoke
		 * @param args
		 * @throws Throwable
		 */
		public void invoke(String[] args) throws Throwable {
			try {
				main.invoke(null, new Object[] { args });
			} catch (Exception except) {
				throw except.getCause();
			}
		}

		public String getDescription() {
			return description;
		}
	}

}
