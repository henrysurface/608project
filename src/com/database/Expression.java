package com.database;

import java.util.List;

import com.database.entity.Node;

import storageManager.FieldType;
import storageManager.Tuple;

public class Expression {

	class Attribute {
		String type;
		String stringValue;
		int integerValue;
		public Attribute() {}
		public boolean equals(Attribute attribute) {
			if (!this.type.equals(attribute.type))
				return false;
			if (this.type.equals("integer")) {
				return this.integerValue == attribute.integerValue;
			} else {
				return this.stringValue.equals(attribute.stringValue);
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
		List<Node> nextNode = node.getChildren();
		Node operationLeft = nextNode.get(0);
		Node operationRight = nextNode.get(1);
		switch (node.getAttr()) {
		case "WHERE": {

			return new Expression(operationLeft).evaluateOperation(tuple);

		}
		case "AND": {
			return new Expression(operationLeft).evaluateOperation(tuple)
					&& new Expression(operationRight).evaluateOperation(tuple);
		}
		case "OR": {
			return new Expression(operationLeft).evaluateOperation(tuple)
					|| new Expression(operationRight).evaluateOperation(tuple);
		}
		case "=": {
			Expression left = new Expression(operationLeft);
			Expression right = new Expression(operationRight);
			return left.evaluateTypes(tuple).equals(right.evaluateTypes(tuple));
		}
		case ">": {
			return new Expression(operationLeft).evaluateIntValue(tuple) > new Expression(operationRight)
					.evaluateIntValue(tuple);
		}
		case "<": {
			return new Expression(operationLeft).evaluateIntValue(tuple) < new Expression(operationRight)
					.evaluateIntValue(tuple);
		}
		case "NOT": {
			return !new Expression(operationLeft).evaluateOperation(tuple);
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

	private Attribute evaluateTypes(Tuple tuple) {
		Attribute temp = new Attribute();
		if (node.getAttr().equals("string")) {
			temp.type = "string";
			temp.stringValue = node.getChildren().get(0).getAttr();
		} else if (node.getAttr().equalsIgnoreCase("integer")) {
			temp.type = "integer";
			temp.integerValue = Integer.parseInt(node.getChildren().get(0).getAttr());
		} else if (node.getAttr().equalsIgnoreCase("attr_name")) {
			StringBuilder fieldName = new StringBuilder();
			for (Node name : node.getChildren()) {
				fieldName.append(name.getAttr() + ".");
			}
			fieldName.deleteCharAt(fieldName.length() - 1);
			String name = fieldName.toString();
			FieldType type = tuple.getSchema().getFieldType(name);
			if (type == FieldType.INT) {
				temp.type = "integer";
				temp.integerValue = tuple.getField(name).integer;
			} else {
				temp.type = "string";
				temp.stringValue = tuple.getField(name).str;
			}
		} else {
			temp.type = "integer";
			temp.integerValue = evaluateIntValue(tuple);
		}
		return temp;
	}

	private int evaluateIntValue(Tuple tuple) {
		List<Node> nextNodes = node.getChildren();
		Node leftValue = nextNodes.get(0);
		Node rightValue = nextNodes.get(1);
		switch (node.getAttr()) {
		case "+": {
			return new Expression(leftValue).evaluateIntValue(tuple)
					+ new Expression(rightValue).evaluateIntValue(tuple);
		}
		case "-": {
			return new Expression(leftValue).evaluateIntValue(tuple)
					- new Expression(rightValue).evaluateIntValue(tuple);
		}
		case "*": {
			return new Expression(leftValue).evaluateIntValue(tuple)
					* new Expression(rightValue).evaluateIntValue(tuple);
		}
		case "/": {
			return new Expression(leftValue).evaluateIntValue(tuple)
					/ new Expression(rightValue).evaluateIntValue(tuple);
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
			return Integer.parseInt(leftValue.getAttr());
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
