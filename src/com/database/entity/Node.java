package com.database.entity;

import java.util.ArrayList;
import java.util.List;

public class Node {
	String attr;
	List<Node> children;
	public Node(String attr) {
		this.attr = attr;
		this.children = new ArrayList<>();
	}
	
	public Node() {
		this.children = new ArrayList<>();
	}
	
	public Node(String attr, boolean end) {
		this.attr = attr;
		this.children = null;
	}

	public String getAttr() {
		return attr;
	}

	public void setAttr(String attr) {
		this.attr = attr;
	}

	public List<Node> getChildren() {
		return children;
	}

	public void setChildren(List<Node> children) {
		this.children = children;
	}
	
	
	
}
