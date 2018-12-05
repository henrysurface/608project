package com.database;

import java.util.List;

import com.database.entity.Node;

import storageManager.FieldType;
import storageManager.Tuple;

public class Expression {

	class Temp {
		String type;
		String tempString;
		int tempInteger;

		public boolean equals(Temp t2) {
			if (!this.type.equalsIgnoreCase(t2.type))
				return false;
			if (this.type.equals("INT")) {
				return this.tempInteger == t2.tempInteger;
			} else {
				return this.tempString.equals(t2.tempString);
			}
		}
	}

	Node node;

	public Expression(Node node) {
		this.node = node;
	}

	public Expression() {
	}

	public boolean evaluateOperation(Tuple tuple) {
//		List<Node> nextNode = node.getChildren().get();
//		Node operationLeft = nextNode.get(0);
//		Node operationRight = nextNode.get(1);
		switch (node.getAttr()) {
		case "WHERE": {

			return new Expression(node.getChildren().get(0)).evaluateOperation(tuple);

		}
		case "AND": {
			return new Expression(node.getChildren().get(0)).evaluateOperation(tuple)
					&& new Expression(node.getChildren().get(1)).evaluateOperation(tuple);
		}
		case "OR": {
			return new Expression(node.getChildren().get(0)).evaluateOperation(tuple)
					|| new Expression(node.getChildren().get(1)).evaluateOperation(tuple);
		}
		case "=": {
			Expression left = new Expression(node.getChildren().get(0));
			Expression right = new Expression(node.getChildren().get(1));
			return left.evaluateTypes(tuple).equals(right.evaluateTypes(tuple));
		}
		case ">": {
			return new Expression(node.getChildren().get(0))
					.evaluateIntValue(tuple) > new Expression(node.getChildren().get(1)).evaluateIntValue(tuple);
		}
		case "<": {
			return new Expression(node.getChildren().get(0))
					.evaluateIntValue(tuple) < new Expression(node.getChildren().get(1)).evaluateIntValue(tuple);
		}
		case "NOT": {
			return !new Expression(node.getChildren().get(0)).evaluateOperation(tuple);
		}
		default:
			try {
				throw new Exception("Unknown Operator");
			} catch (Exception err) {
				err.printStackTrace();
			}
		}
		return false;
	}

	private Temp evaluateTypes(Tuple tuple) {
		Temp temp = new Temp();
		if (node.getAttr().equalsIgnoreCase("STRING")) {
			temp.type = "STRING";
			temp.tempString = node.getChildren().get(0).getAttr();
		} else if (node.getAttr().equalsIgnoreCase("INT")) {
			temp.type = "INT";
			temp.tempInteger = Integer.parseInt(node.getChildren().get(0).getAttr());
		} else if (node.getAttr().equalsIgnoreCase("ATTR_NAME")) {
			StringBuilder fieldName = new StringBuilder();
			for (Node name : node.getChildren()) {
				fieldName.append(name.getAttr() + ".");
			}
			fieldName.deleteCharAt(fieldName.length() - 1);
			String name = fieldName.toString();
			FieldType type = tuple.getSchema().getFieldType(name);
			if (type == FieldType.INT) {
				temp.type = "INT";
				temp.tempInteger = tuple.getField(name).integer;
			} else {
				temp.type = "STRING";
				temp.tempString = tuple.getField(name).str;
			}
		} else {
			temp.type = "INT";
			temp.tempInteger = evaluateIntValue(tuple);
		}
		return temp;
	}

	private int evaluateIntValue(Tuple tuple) {
		List<Node> nextNodes = node.getChildren();
//		Node leftValue = nextNodes.get(0);
//		Node rightValue = nextNodes.get(1);
		switch (node.getAttr()) {
		case "+": {
			return new Expression(node.getChildren().get(0)).evaluateIntValue(tuple)
					+ new Expression(node.getChildren().get(1)).evaluateIntValue(tuple);
		}
		case "-": {
			return new Expression(node.getChildren().get(0)).evaluateIntValue(tuple)
					- new Expression(node.getChildren().get(1)).evaluateIntValue(tuple);
		}
		case "*": {
			return new Expression(node.getChildren().get(0)).evaluateIntValue(tuple)
					* new Expression(node.getChildren().get(1)).evaluateIntValue(tuple);
		}
		case "/": {
			int left = new Expression(node.getChildren().get(0)).evaluateIntValue(tuple);
			int right = new Expression(node.getChildren().get(1)).evaluateIntValue(tuple);
			return right == 0 ? 0 : left / right;
		}
		case "attr_name": {
			StringBuilder fieldName = new StringBuilder();
			for (Node name : nextNodes) {
				fieldName.append(name.getAttr() + ".");
			}
			fieldName.deleteCharAt(fieldName.length() - 1);
			String name = fieldName.toString();
			return tuple.getField(name).integer;
		}
		case "integer": {
			return Integer.parseInt(node.getChildren().get(0).getAttr());
		}
		}
		return 0;
	}

	public Node getNode() {
		return node;
	}

	public void setNode(Node node) {
		this.node = node;
	}

}
