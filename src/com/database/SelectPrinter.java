package com.database;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.database.entity.Node;

import storageManager.Block;
import storageManager.Disk;
import storageManager.FieldType;
import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.Schema;
import storageManager.SchemaManager;
import storageManager.Tuple;

public class SelectPrinter {

	private static MainMemory memory = new MainMemory();
	private static Disk disk = new Disk();
	private static SchemaManager schemaManager = new SchemaManager(memory, disk);
	Node col = null, from = null, order = null;
	Expression expression = null;

	public SelectPrinter(List<Node> nodes) {
		for (Node node : nodes) {
			if (node.getAttr().equals("attr_detail"))
				col = node;
			if (node.getAttr().equals("from"))
				from = node;
			if (node.getAttr().equals("where"))
				expression = new Expression(node);
			if (node.getAttr().equals("order"))
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
		}
		// multiple-relation
		else {

		}
	}

	private void selectSingleRelation(BufferedWriter bw) throws IOException {
		boolean distinct = false;
		if (col.getChildren().get(0).getAttr().equals("distinct")) {
			col = col.getChildren().get(0);
			distinct = true;
		}
		ArrayList<String> fieldList = new ArrayList<>();
		getFieldList(col, fieldList);
		assert from.getChildren().get(0).getAttr().equalsIgnoreCase("table");
		String relationName = from.getChildren().get(0).getChildren().get(0).getAttr();
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

		SelectHelper.project(relation, memory, col, expression, bw);

		if (tempRelations.isEmpty())
			return;
		for (String temp : tempRelations) {
			if (schemaManager.relationExists(temp))
				schemaManager.deleteRelation(temp);
		}

//		if (relation.getNumOfBlocks() <= memory.getMemorySize()) {
//			if (!printTupleWithinMemorySize(col, order, expression, fieldList, bw, muti, distinct, distinctNodes,
//					targetNodes, relation)) {
//				return;
//			}
//		} else {
//			printTuplesLargerThanMemorySize(col, order, distinct, relationName, expression);
//		}
	}

	private void printTuplesLargerThanMemorySize(Node col, Node order, String distinct, String relationName,
			Expression expression) {
		ArrayList<String> fields = new ArrayList<>();
		boolean distincts = false;
		if (distinct.equalsIgnoreCase("DISTINCT")) {
			distincts = true;
			col = col.getChildren().get(0);
		}

		for (Node field : col.getChildren()) {
			if (field.getChildren().size() == 1) { // attr
				fields.add(field.getChildren().get(0).getAttr());
			} else { // table.attr
				fields.add(field.getChildren().get(0).getAttr() + "." + field.getChildren().get(1).getAttr());
			}

		}
		if (order == null && !distincts) {
			// basic select operation with/without where.
			System.out.println("Basic select operation with/without where");
			basicSelect(relationName, fields, expression);
		} else {
			// select from one table in order/distinct
			System.out.println("Select from one table in order/distinct");
			String orderField = order == null ? null : order.getChildren().get(0).getChildren().get(0).getAttr();
			advancedSelect(relationName, fields, expression, orderField, distincts);
		}
	}

	private void advancedSelect(String relationName, ArrayList<String> fields, Expression exp, String orderBy,
			boolean distincts) {
		Relation relation = schemaManager.getRelation(relationName);
		if (exp != null) {
			relation = setUpRelationWithExpression(relationName, exp, relation);
		}

		System.out.println("Number of tuples: " + relation.getNumOfTuples());
		/**
		 * This part ends here
		 */

		if (relation.getNumOfBlocks() <= memory.getMemorySize()) {
			relation.getBlocks(0, 0, relation.getNumOfBlocks());
			ArrayList<Tuple> tuples = memory.getTuples(0, relation.getNumOfBlocks());
			Algorithms.sortInMemory(tuples, orderBy);
			if (distincts) {
				Algorithms.removeDuplicate(tuples, fields);
			}
			printTitle(tuples.get(0), fields);
			for (Tuple tuple : tuples) {
				// System.out.println("619 ");
				print(tuple, fields);
			}
		} else {
			System.out.println("Two pass condition");
			ArrayList<String> order = new ArrayList<>();
			if (orderBy != null) {
				order.add(orderBy);
			}
			if (fields.get(0).equals("*")) {
				// System.out.print( "546 ");
				fields = relation.getSchema().getFieldNames();
			}
			if (distincts && orderBy != null) {
				relation = Helper.executeDistinct(schemaManager, memory, relation, fields, 1);
				Helper.executeOrder(schemaManager, memory, relation, order, 0);
			} else if (distincts) {
				Helper.executeDistinct(schemaManager, memory, relation, fields, 0);
			} else if (orderBy != null) {
				Helper.executeOrder(schemaManager, memory, relation, order, 0);
			}
		}

	}

	private Relation setUpRelationWithExpression(String relationName, Expression exp, Relation relation) {
		Schema schema = relation.getSchema();
		Relation tempRelation = schemaManager.createRelation(relationName + "temp", schema);
		int tempRelationCurrentBlock = 0;
		Block tempBlock = memory.getBlock(1);
		tempBlock.clear();
		for (int i = 0; i < relation.getNumOfBlocks(); i++) {
			relation.getBlock(i, 0);
			ArrayList<Tuple> tupes = memory.getBlock(0).getTuples();
			for (Tuple tupe : tupes) {
				if (exp.evaluateOperation(tupe)) {
					if (!tempBlock.isFull())
						tempBlock.appendTuple(tupe);
					else {
						memory.setBlock(1, tempBlock);
						tempRelation.setBlock(tempRelationCurrentBlock, 1);
						tempRelationCurrentBlock++;
						tempBlock.clear();
						tempBlock.appendTuple(tupe);
					}
				}
			}
		}

		if (!tempBlock.isEmpty()) {
			memory.setBlock(1, tempBlock);
			tempRelation.setBlock(tempRelationCurrentBlock, 1);
			tempBlock.clear();
		}
		relation = tempRelation;
		return relation;
	}

	private void printTitle(Tuple tuple, ArrayList<String> fields) {
		if (fields.get(0).equals("*")) {
			// System.out.print( "467 ");
			for (String fieldNames : tuple.getSchema().getFieldNames()) {
				System.out.print(fieldNames + "   ");
			}
			System.out.println();
		} else {
			for (String str : fields) {
				// System.out.print("544 ");
				System.out.print(str + "    ");
			}
			System.out.println();
		}
	}

	private void basicSelect(String relationName, ArrayList<String> fields, Expression expression) {
		int currentBlockCount = 0;
		Relation relation = schemaManager.getRelation(relationName);
		boolean show = false;
		while (currentBlockCount < relation.getNumOfBlocks()) {
			int readBlocks = relation.getNumOfBlocks() - currentBlockCount > memory.getMemorySize()
					? memory.getMemorySize()
					: relation.getNumOfBlocks() - currentBlockCount;
			relation.getBlocks(currentBlockCount, 0, readBlocks);
			ArrayList<Tuple> tuples = memory.getTuples(0, readBlocks);
			if (!show) {
				show = true;
				if (fields.get(0).equals("*")) {
					for (String fieldNames : tuples.get(0).getSchema().getFieldNames()) {
						System.out.print(fieldNames + "   ");
					}
					System.out.println();
				} else {
					for (String name : fields)
						System.out.print(name + "  ");
					System.out.println();
				}
			}
			for (Tuple tuple : tuples) {
				if (expression == null)
					print(tuple, fields);
				else {
					if (expression.evaluateOperation(tuple))
						print(tuple, fields);
				}
			}
			currentBlockCount += readBlocks;
		}

	}

	private void print(Tuple tuple, ArrayList<String> fields) {
		if (fields.get(0).equals("*")) {
			System.out.println(tuple);
			return;
		}

		for (String field : fields) {
			if (field.indexOf('.') > 0) { // table.attr
				String tmp_field = field.substring(field.indexOf('.') + 1);
				System.out.print(
						(tuple.getSchema().getFieldType(tmp_field) == FieldType.INT ? tuple.getField(tmp_field).integer
								: tuple.getField(tmp_field).str) + "   ");
			} else { // attr
				System.out.print((tuple.getSchema().getFieldType(field) == FieldType.INT ? tuple.getField(field).integer
						: tuple.getField(field).str) + "   ");
			}
		}

		System.out.println();
	}

	private boolean printTupleWithinMemorySize(Node col, Node order, Expression expression, ArrayList<String> fieldList,
			BufferedWriter bw, String muti, String distinct, List<Node> distinctNodes, List<Node> targetNodes,
			Relation relation) throws IOException {
		relation.getBlocks(0, 0, relation.getNumOfBlocks());
		ArrayList<Tuple> tuples = new ArrayList<>();
		if (expression != null) {
			ArrayList<Tuple> where = new ArrayList<>();
			for (Tuple tuple : tuples) {
				if (expression.evaluateOperation(tuple)) {
					where.add(tuple);
				}
			}
			tuples.addAll(where);
		}
		if (order != null || col.getChildren().get(0).getAttr().equalsIgnoreCase("DISTINCT")) {
			// Make them in order
			if (tuples.size() == 0) {
				System.out.println("Empty Table");
				return false;
			}
			Algorithms.sortInMemory(tuples,
					order == null ? null : order.getChildren().get(0).getChildren().get(0).getAttr());

			if (col.getChildren().get(0).getAttr().equalsIgnoreCase("DISTINCT")) {
				Algorithms.removeDuplicate(tuples, fieldList);
			}
		}

		if (muti.equals("*")) {
			printMuti(tuples, bw);
		} else {
			if (distinct.equals("distinct")) {
				printField(bw, distinctNodes);
			} else {
				printField(bw, targetNodes);
			}
		}
		System.out.println();
		bw.write("\n");
		for (Tuple t : tuples) {
			if (distinct.equalsIgnoreCase("DISTINCT")) { // has distinct
				printValuesFromTuples(bw, distinctNodes, t);
			} else { // without distinct
				printValuesFromTuples(bw, targetNodes, t);
			}
			System.out.println();
			bw.write("\n");
		}
		System.out.println("--------------------------------");
		bw.write("--------------------------------\n\n\n");
		return true;
	}

	private void printMuti(ArrayList<Tuple> tuples, BufferedWriter bw) throws IOException {
		try {
			for (String s : tuples.get(0).getSchema().getFieldNames()) {
				System.out.print(s + "  ");
				bw.write(s + "  ");
			}
			System.out.println();
			bw.write("\n");
			for (Tuple t : tuples) {
				System.out.println(t);
				String tuplestring = t.toString(false);
				bw.write(tuplestring + "\n");
			}
		} catch (Exception exp) {
			System.out.println("No tuples");
			bw.write("No tuples" + "\n");
		}
	}

	private void printValuesFromTuples(BufferedWriter bw, List<Node> distinctNodes, Tuple t) throws IOException {
		for (Node field : distinctNodes) {
			if (field.getChildren().size() == 1) {
				if (t.getSchema().getFieldType(field.getChildren().get(0).getAttr()) == FieldType.INT) {

					System.out.print(t.getField(field.getChildren().get(0).getAttr()).integer + "   ");

					bw.write(t.getField(field.getChildren().get(0).getAttr()).integer + "   ");

				} else {

					System.out.print(t.getField(field.getChildren().get(0).getAttr()).str + "   ");

					bw.write(t.getField(field.getChildren().get(0).getAttr()).str + "   ");

				}
			} else {
				if (t.getSchema().getFieldType(field.getChildren().get(1).getAttr()) == FieldType.INT) {

					System.out.print(t.getField(field.getChildren().get(1).getAttr()).integer + "   ");

					bw.write(t.getField(field.getChildren().get(1).getAttr()).integer + "   ");
				} else {

					System.out.print(t.getField(field.getChildren().get(1).getAttr()).str + "   ");

					bw.write(t.getField(field.getChildren().get(1).getAttr()).str + "   ");

				}
			}
		}
	}

	private void printField(BufferedWriter bw, List<Node> nodes) throws IOException {
		for (Node field : nodes) {
			if (field.getChildren().size() == 1) { // attr
				System.out.print(field.getChildren().get(0).getAttr() + "  ");
				bw.write(field.getChildren().get(0).getAttr() + "  ");
			} else { // table.attr
				System.out.print(
						field.getChildren().get(0).getAttr() + "." + field.getChildren().get(1).getAttr() + "  ");
				bw.write(field.getChildren().get(0).getAttr() + "." + field.getChildren().get(1).getAttr() + "  ");
			}
		}
	}

	private void getFieldList(Node col, ArrayList<String> fieldList) {
		for (Node node : col.getChildren()) {
			assert node.getAttr().equals("attr_name");
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
		if (col.getChildren().get(0).getAttr().equals("distinct")) {
			col = col.getChildren().get(0);
			distinct = true;
		}
		ArrayList<String> fieldList = new ArrayList<>();
		getFieldList(col, fieldList);
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
