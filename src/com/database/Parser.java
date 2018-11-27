package com.database;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import com.database.entity.Node;

public class Parser {

	public Node statementFactory(String[] sql) {
		Node node = new Node();
		String firstComm = sql[0];
		try {
			if (firstComm.equals("create")) {
				createParser(sql, node);
			} else if (firstComm.equals("drop")) {
				dropParser(sql, node);
			} else if (firstComm.equals("delete")) {
				deleteParser(sql, node);
			} else if (firstComm.equals("insert")) {
				insertParser(sql, node);
			} else if (firstComm.equals("select")) {
				selectParser(sql, node);
			} else {
				throw new IllegalArgumentException("Invalid sql");
			}
		} catch (IllegalArgumentException e) {
			System.out.println(e.getMessage());
		}

		return node;
	}

	private void selectParser(String[] sql, Node node) {
		// TODO Auto-generated method stub
		node.setAttr("select");

		int fromIndex = 0, whereIndex = 0, orderByIndex = 0;
		for (int i = 1; i < sql.length; i++) {
			if (sql[i].equals("from")) {
				fromIndex = i;
				node.getChildren().add(attrList(Arrays.copyOfRange(sql, 1, fromIndex)));
			} else if (sql[i].equals("where")) {
				whereIndex = i;
				node.getChildren().add(fromNode(Arrays.copyOfRange(sql, fromIndex + 1, whereIndex)));
			} else if (sql[i].equals("order")) {
				orderByIndex = i;
				if (whereIndex != 0) {
					node.getChildren().add(whereNode(Arrays.copyOfRange(sql, whereIndex + 1, orderByIndex)));
				} else if (whereIndex == 0) {
					node.getChildren().add(fromNode(Arrays.copyOfRange(sql, fromIndex + 1, orderByIndex)));
				}
				node.getChildren().add(orderNode(Arrays.copyOfRange(sql, orderByIndex + 2, sql.length)));
			}
		}
		if (orderByIndex == 0 && whereIndex != 0) {
			node.getChildren().add(whereNode(Arrays.copyOfRange(sql, whereIndex + 1, sql.length)));
		}
		if (orderByIndex == 0 && whereIndex == 0) {
			node.getChildren().add(fromNode(Arrays.copyOfRange(sql, fromIndex + 1, sql.length)));
		}
	}

	private Node orderNode(String[] sql) {
		Node node = new Node("order");
		node.getChildren().add(leafAttrName(sql[0]));
		return node;
	}

	private Node fromNode(String[] sql) {
		Node node = new Node("from");
		for (String elem : sql) {
			if (elem.charAt(elem.length() - 1) == ',') {
				elem = elem.substring(0, elem.length() - 1);
			}
			node.getChildren().add(leafTable(elem));
		}

		return node;
	}

	private void insertParser(String[] sql, Node node) {

		int valueIndex = 0;
		int selectIndex = 0;
		// get index of VALUES and SELECT
		for (int i = 0; i < sql.length; i++) {
			if (sql[i].equals("values"))
				valueIndex = i;
			if (sql[i].equals("select"))
				selectIndex = i;
		}
		if (valueIndex == 0 && selectIndex == 0) {
			throw new IllegalArgumentException("Invalid sql! please check the insert statement.");
		}

		if (valueIndex > 0) {
			node.setAttr("insert");
			node.getChildren().add(leafTable(sql[2]));
			node.getChildren().add(attrList(Arrays.copyOfRange(sql, 3, valueIndex)));
			node.getChildren().add(values(Arrays.copyOfRange(sql, valueIndex + 1, sql.length)));
		}

		else if (selectIndex > 0) {
			node.setAttr("insert");
			node.getChildren().add(leafTable(sql[2]));
			node.getChildren().add(attrList(Arrays.copyOfRange(sql, 3, selectIndex)));
			Node selectNode = new Node("select");
			selectParser(Arrays.copyOfRange(sql, selectIndex, sql.length), selectNode);
			node.getChildren().add(selectNode);
		}

	}

	private Node values(String[] sql) {
		Node node = new Node("values");

		for (String elem : sql) {
			String item = trim(elem);
			node.getChildren().add(leafValue(item));
		}
		return node;
	}

	private Node leafValue(String item) {
		Node node = new Node("value");
		node.getChildren().add(new Node(item, true));
		return node;
	}

	private Node attrList(String[] sql) {
		Node node = new Node("attr_list");
		if (sql[0].equals("distinct")) {
			node.getChildren().add(distinctNode(Arrays.copyOfRange(sql, 1, sql.length)));
		} else {
			for (String elem : sql) {
				if (elem.length() > 0) {
					if (elem.charAt(0) == '(') {
						elem = elem.substring(1, elem.length());
					}
					if (elem.charAt(elem.length() - 1) == ')' || elem.charAt(elem.length() - 1) == ',') {
						elem = elem.substring(0, elem.length() - 1);
					}
					node.getChildren().add(leafAttrName(elem));
				}
			}
		}
		return node;
	}

	private Node distinctNode(String[] sql) {
		Node node = new Node("distinct");
		for (int i = 0; i < sql.length; i++) {
			String elem = sql[i];
			if (elem.length() > 0) {
				node.getChildren().add(leafAttrName(trim(elem)));

			}
		}
		return node;
	}

	private void deleteParser(String[] sql, Node node) {
		node.setAttr("delete");
		String table = sql[2];
		node.getChildren().add(leafTable(table));
		if (sql.length > 3 && sql[3].equalsIgnoreCase("WHERE")) {
			node.getChildren().add(whereNode(Arrays.copyOfRange(sql, 4, sql.length)));
		}

	}

	private Node whereNode(String[] sql) {
		Node node = new Node("where");
		node.getChildren().add(searchCondition(sql));
		return node;
	}

	private Node searchCondition(String[] sql) {
		Map<String, Integer> priority = new HashMap<String, Integer>();
		priority.put("OR", 0);
		priority.put("AND", 1);
		priority.put("=", 2);
		priority.put(">", 2);
		priority.put("<", 2);
		priority.put("+", 3);
		priority.put("-", 3);
		priority.put("*", 4);
		priority.put("/", 4);

		Stack<Node> sqlStack = new Stack<Node>();
		for (int i = 0; i < sql.length; i++) {
			if (priority.containsKey(sql[i])) {
				if (sqlStack.size() >= 3) {
					Node last = sqlStack.pop();
					if (priority.get(sql[i]) >= priority.get(sqlStack.peek().getAttr())) {
						sqlStack.push(last);
						sqlStack.push(new Node(sql[i]));
					} else {
						while (sqlStack.size() > 0 && priority.get(sqlStack.peek().getAttr()) > priority.get(sql[i])) {
							Node operation = sqlStack.pop();
							Node operation2 = sqlStack.pop();
							operation.getChildren().add(operation2);
							operation.getChildren().add(last);
							last = operation;
						}

						sqlStack.push(last);
						sqlStack.push(new Node(sql[i]));
					}
				} else {
					sqlStack.push(new Node(sql[i]));
				}
			} else if (tryInteger(sql[i])) {
				sqlStack.push(leafInteger(sql[i]));
			} else if (sql[i].charAt(0) == '"') {
				sqlStack.push(leafString(sql[i].substring(1, sql[i].length() - 1)));
			} else if (sql[i].charAt(0) == '(') {
				String[] subWhereOperation = new String[3];
				subWhereOperation[0] = trim(sql[i]);
				subWhereOperation[1] = sql[i + 1];
				subWhereOperation[2] = trim(sql[i + 2]);
				sqlStack.push(searchCondition(subWhereOperation));
				i = i + 2;
			} else {
				sqlStack.push(leafAttrName(sql[i]));
			}
		}

		if (sqlStack.size() >= 3) {
			Node currentElem = sqlStack.pop();

			while (sqlStack.size() >= 2) {
				Node node = sqlStack.pop();
				node.getChildren().add(sqlStack.pop());
				node.getChildren().add(currentElem);
				currentElem = node;
			}
			return currentElem;
		} else {
			return sqlStack.peek();
		}
	}

	private Node leafString(String sql) {
		Node node = new Node("STRING");
		node.getChildren().add(new Node(sql, true));
		return node;
	}

	private Node leafInteger(String sql) {
		Node node = new Node("INT");
		node.getChildren().add(new Node(sql, true));
		return node;
	}

	private boolean tryInteger(String string) {
		try {
			Integer.parseInt(string);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	private void dropParser(String[] sql, Node node) {
		node.setAttr("drop");
		node.getChildren().add(leafTable(sql[2]));
	}

	private void createParser(String[] sql, Node node) {
		node.setAttr("create");
		node.getChildren().add(leafTable(sql[2]));
		node.getChildren().add(createTableAttr(Arrays.copyOfRange(sql, 3, sql.length)));
	}

	private Node createTableAttr(String[] sql) {
		Node node = new Node("create_attr");
		if (sql.length % 2 != 0) {
			System.out.println("ERROR! invaild attributes format");
		}

		for (int i = 0; i < sql.length / 2; i++) {
			node.getChildren().add(attributeDetail(Arrays.copyOfRange(sql, 2 * i, 2 * i + 2)));
		}

		return node;
	}

	private Node attributeDetail(String[] sql) {
		Node node = new Node("attr_detail");
		String attrName = sql[0];
		String attrType = sql[1];
		if (attrName.charAt(0) == '(' || attrName.charAt(0) == ',') {
			attrName = attrName.substring(1);
		}
		if (attrName.charAt(attrName.length() - 1) == ',' || attrName.charAt(attrName.length() - 1) == ')') {
			attrName = attrName.substring(0, attrName.length() - 1);
		}
		if (attrType.charAt(attrType.length() - 1) == ',' || attrType.charAt(attrType.length() - 1) == ')') {
			attrType = attrType.substring(0, attrType.length() - 1);
		}

		node.getChildren().add(leafAttrName(attrName));
		node.getChildren().add(leafAttrType(attrType));

		return node;
	}

	private Node leafAttrType(String attrType) {
		Node node = new Node("attr_type");
		node.getChildren().add(new Node(attrType, true));
		return node;
	}

	private Node leafAttrName(String attrName) {
		Node node = new Node("attr_name");
		String[] name = attrName.split("\\.");
		for (String elemInName : name) {
			node.getChildren().add(new Node(elemInName, true));
		}
		return node;
	}

	private Node leafTable(String tableName) {
		Node leaf = new Node("table");
		leaf.getChildren().add(new Node(tableName, true));
		return leaf;
	}

	private String trim(String elem) {
		String str = elem;
		if (str.length() == 0)
			return null;
		if (str.charAt(0) == '(')
			str = str.substring(1);
		if (str.charAt(0) == '"')
			str = str.substring(1);
		if (str.charAt(str.length() - 1) == ')')
			str = str.substring(0, str.length() - 1);
		if (str.charAt(str.length() - 1) == ',')
			str = str.substring(0, str.length() - 1);
		if (str.charAt(str.length() - 1) == '"')
			str = str.substring(0, str.length() - 1);
		return str;
	}
}
