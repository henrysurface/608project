package com.database.entity;

import storageManager.FieldType;

public class Attribute {

	String name;
	FieldType type;
	
	public Attribute() {};
	
	
	public Attribute(String name, FieldType type) {
		this.name = name;
		this.type = type;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public FieldType getType() {
		return type;
	}
	public void setType(FieldType type) {
		this.type = type;
	}

	
}
