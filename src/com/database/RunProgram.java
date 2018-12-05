package com.database;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Scanner;

import com.database.entity.Node;

public class RunProgram {

	public static void main(String[] args) {
		boolean flag = true;
		Parser parser = new Parser();
		Scanner in = new Scanner(System.in);
		while (flag) {
			System.out.println("------------------TinySQL------------------");
			System.out.println("Please enter a mode to start:");
			System.out
					.println(" -f: Query with file read mode.\n -c: Query with commend line mode.\n -exit: exit test\n");
			String res = in.nextLine();
			Query query = new Query();
			switch (res) {
			case "-f":
				System.out.println("------------------Query Mode------------------");
				System.out.println("Input \"exit\" to exit file read mode\n\nPlease input file name:");
				String fileName = in.nextLine();

				while (!fileName.equalsIgnoreCase("-exit")) {
					// read commands in the file
					File file = new File(fileName);
					List<String> commandList = Helper.fileReader(file);
					for (String com : commandList) {
						String[] sql = com.split(" ");
						Node node = null;
						try {
							node = parser.statementFactory(sql);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
							flag =false;
						}
						
						
						 //print parsing results 
						String attr = node.getAttr(); 
						List<Node> list = node.getChildren(); 
						System.out.println(attr);
						nodeDisplay(list);
						
						try {
							query.queryExcutor(node);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					// system waiting
					System.out.println("-----------------File Read Mode---------------");
					System.out.println("Enter \"-exit\" to exit mode\n\nPlease enter file name:");
					fileName = in.nextLine();
				}

				break;
			case "-c":
				System.out.println("-----------------Command Line Mode-----------------");
				System.out.println("Enter \"exit\" to exit command line mode\n\nPlease enter command:");
				String command = in.nextLine();
				while (!command.equalsIgnoreCase("-exit")) {
					String[] sql = command.split(" ");
					// System.out.println("The command you just input is "+command);
					Node node = parser.statementFactory(sql);

					String attr = node.getAttr(); 
					List<Node> list = node.getChildren(); 
					System.out.println(attr);
					nodeDisplay(list);
					
					try {
						query.queryExcutor(node);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					System.out.println("--------------------Command Line Mode------------------");
					System.out.println("Input \"-exit\" to exit command line mode\n\nPlease input command:");
					command = in.nextLine();
					// iterator.execute(parser.parse(command));
				}
				break;
			case "-exit":
				flag = false;
				break;
			default:
				System.out.println("Please enter -c or -f to start query.");
				res = in.nextLine();
			}
		}
	}

	public static void nodeDisplay(List<Node> nodes) {
		for (Node node : nodes) {
			String attr = node.getAttr();
			System.out.println(attr);
			if (node.getChildren() != null) {
				//System.out.println(">>>new node");
				nodeDisplay(node.getChildren());
				//System.out.println(">>>end child node");
			}
		}
		///Users/henry/Documents/programming/spring/608project/DatabaseProject2/src/com/database/TinySQL-TextLINUX.txt
	}

}
