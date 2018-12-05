package com.database;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.database.entity.Node;

import storageManager.Disk;
import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.SchemaManager;

public class SelectPrinter {

	private static MainMemory memory = new MainMemory();
	private static Disk disk = new Disk();
	private static SchemaManager schemaManager = new SchemaManager(memory, disk);
	Node col = null, from = null, order = null;
	Expression expression = null;

	public SelectPrinter(List<Node> nodes, MainMemory memory2, Disk disk2, SchemaManager schemaManager2) {
		memory = memory2;
		disk = disk2;
		schemaManager = schemaManager2;
		
		for (Node node : nodes) {
			if (node.getAttr().equalsIgnoreCase("ATTR_LIST"))
				col = node;
			if (node.getAttr().equalsIgnoreCase("FROM"))
				from = node;
			if (node.getAttr().equalsIgnoreCase("WHERE"))
				expression = new Expression(node);
			if (node.getAttr().equalsIgnoreCase("ORDER"))
				order = node;
		}
		assert col != null;
		assert from != null;
	}

	public void runSelect() throws IOException {

		FileWriter fw = new FileWriter("Result.txt", true);
		BufferedWriter bw = new BufferedWriter(fw);
		String content = "--------------------------------\n";
		String contentWithEnter = "--------------------------------";
		System.out.println(contentWithEnter);
		bw.write(content);
		// single relation
		if (from.getChildren().size() == 1) {
			selectSingleRelation(bw);
			//System.out.println("Here");
		}
		// multiple-relation
		else {
			//System.out.println("AAA");
			selectMultipleRelation(bw);
			
		}
	}

	private void selectMultipleRelation(BufferedWriter bw) throws IOException{
		int tableNum = from.getChildren().size();
		String[] tables = new String[tableNum];
		for (int i = 0; i < tableNum; i++) {
			tables[i] = from.getChildren().get(i).getChildren().get(0).getAttr();
		}
		if (expression == null) {
			//no where condition, doing cross join
			Relation product = PhysicalQueryPlan.crossJoin(schemaManager, memory, tables);
			SelectHelper.outputRelation(product, memory, col, expression, bw);
		}
		else {
			//where condition with equal condition, doing natural join
			String commonAttr = expression.getNode().getChildren().get(0).getChildren().get(0).getChildren().get(1).getAttr();
			Relation naturalJoinResult = PhysicalQueryPlan.naturalJoin(schemaManager, memory, tables[0], tables[1], commonAttr);
			SelectHelper.outputRelation(naturalJoinResult, memory, col, expression, bw);
		}
		
	}

	private void selectSingleRelation(BufferedWriter bw) throws IOException {
		boolean distinct = false;
		//boolean star = false;
		if (col.getChildren().get(0).getAttr().equalsIgnoreCase("DISTINCT")) {
			col = col.getChildren().get(0);
			distinct = true;
			//System.out.println("dict Here");
		}
		ArrayList<String> fieldList = new ArrayList<>();
		getFieldList(col, fieldList);
		assert from.getChildren().get(0).getAttr().equalsIgnoreCase("TABLE");
		String relationName = from.getChildren().get(0).getChildren().get(0).getAttr();
		Relation relation = schemaManager.getRelation(relationName);
		ArrayList<String> tempRelations = new ArrayList<>();

		if (distinct) {
			relation = SelectHelper.distinct(schemaManager, relation, memory, fieldList);
			SelectHelper.clearMemory(memory);
			tempRelations.add(relation.getRelationName());
			//System.out.println("dict Here");
		}
		if (expression != null) {
			relation = SelectHelper.select(schemaManager, relation, memory, col, order, expression);
			SelectHelper.clearMemory(memory);
			tempRelations.add(relation.getRelationName());
			System.out.println("Expression Here");
		}

		if (order != null) {
			relation = SelectHelper.sort(schemaManager, relation, memory, col, order, expression);
			SelectHelper.clearMemory(memory);
			tempRelations.add(relation.getRelationName());
			System.out.println("Order Here");
		}

		SelectHelper.outputRelation(relation, memory, col, expression, bw);

		if (tempRelations.isEmpty())
			return;
		for (String temp : tempRelations) {
			if (schemaManager.relationExists(temp))
				schemaManager.deleteRelation(temp);
		}
	}

	
	private void getFieldList(Node col, ArrayList<String> fieldList) {
		for (Node node : col.getChildren()) {
			StringBuilder fieldName = new StringBuilder();
			for (Node field : node.getChildren()) {
				fieldName.append(field.getAttr() + ".");
			}
			fieldName.deleteCharAt(fieldName.length() - 1);
			fieldList.add(fieldName.toString());
		}
	}

	public void insertSelectRelation(String relationNameFromInsert, List<Node> colList) {
		boolean distinct = false;
		if (col.getChildren().get(0).getAttr().equalsIgnoreCase("DISTINCT")) {
			col = col.getChildren().get(0);
			distinct = true;
		}
		ArrayList<String> fieldList = new ArrayList<>();
		assert from.getChildren().get(0).getAttr().equalsIgnoreCase("table");
		String relationName = from.getChildren().get(0).getChildren().get(0).getAttr();
		if(!relationNameFromInsert.equalsIgnoreCase(relationName)) {
			System.out.println("Invalid sub select qury.");
			return;
		}
		Relation relation = schemaManager.getRelation(relationName);
		ArrayList<String> tempRelations = new ArrayList<>();

		if (distinct) {
			relation = SelectHelper.distinct(schemaManager, relation, memory, fieldList);
			SelectHelper.clearMemory(memory);
			tempRelations.add(relation.getRelationName());
		}
		if (expression != null) {
			relation = SelectHelper.select(schemaManager, relation, memory, col, order, expression);
			SelectHelper.clearMemory(memory);
			tempRelations.add(relation.getRelationName());
		}

		if (order != null) {
			relation = SelectHelper.sort(schemaManager, relation, memory, col, order, expression);
			SelectHelper.clearMemory(memory);
			tempRelations.add(relation.getRelationName());
		}

		SelectHelper.projectInsert(relation, memory, colList, expression);

		if (tempRelations.isEmpty())
			return;
		for (String temp : tempRelations) {
			if (schemaManager.relationExists(temp))
				schemaManager.deleteRelation(temp);
		}
		
	}

}
