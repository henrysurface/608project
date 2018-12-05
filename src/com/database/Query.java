package com.database;

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

public class Query {

	// private static Parser parser = new Parser();
	private static MainMemory memory = new MainMemory();
	private static Disk disk = new Disk();
	private static SchemaManager schemaManager = new SchemaManager(memory, disk);

	private static void reset() {
		memory = new MainMemory();
		disk = new Disk();
		schemaManager = new SchemaManager(memory, disk);
		disk.resetDiskIOs();
		disk.resetDiskTimer();
	}

	public void queryExecutor(Node node) throws IOException {
		String statement = node.getAttr();
		switch (statement) {
		case "CREATE":
			createQuery(node.getChildren());
			//System.out.println("create");
			break;
		case "DROP":
			dropQuery(node.getChildren());
			break;
		case "DELETE":
			deleteQuery(node.getChildren());
			break;
		case "SELECT":
			selectQuery(node.getChildren());
			break;
		case "INSERT":
			insertQuery(node.getChildren());
			break;
		default:
			System.out.println("Not a legal command!");
		}

	}

	private void deleteQuery(List<Node> node) {

		Node table = null;
		Node condition = null;
		for (Node item : node) {
			if (item.getAttr().equalsIgnoreCase("table"))
				table = item;
			else if (item.getAttr().equalsIgnoreCase("where"))
				condition = item;
		}
		assert table != null;
		if (condition == null) {
			Relation del_relation = schemaManager.getRelation(table.getChildren().get(0).getAttr());
			del_relation.deleteBlocks(0);
		} else {
			Relation del_relation = schemaManager.getRelation(table.getChildren().get(0).getAttr());
			Expression del_condition = new Expression(condition);
			int blk_num = del_relation.getNumOfBlocks();
			for (int i = 0; i < blk_num; i++) {
				boolean deleted = false;
				del_relation.getBlock(i, 0);
				ArrayList<Tuple> find_tuples = memory.getBlock(0).getTuples();
				for (int j = 0; j < find_tuples.size(); j++) {
					if (del_condition.evaluateOperation(find_tuples.get(j))) {
						memory.getBlock(0).invalidateTuple(j);
						deleted = true;
					}
				}
				if (deleted) {
					del_relation.setBlock(i, 0);
					System.out.println("Successfully delete relation.");
				}
			}
		}

		return;

	}

	private void dropQuery(List<Node> children) {
		String dropRelation = children.get(0).getChildren().get(0).getAttr();
		schemaManager.deleteRelation(dropRelation);
		System.out.println("Successfully drop relation: " + dropRelation);
		return;
	}

	public void createQuery(List<Node> node) {
		ArrayList<String> fieldName = new ArrayList<>();
		ArrayList<FieldType> fieldType = new ArrayList<>();
		Node table = node.get(0);
		String tableName = table.getAttr();
		if (!tableName.equalsIgnoreCase("table")) {
			System.out.println("Invalid sql: please type 'table' after create term.");
			return;
		}
		String relationName = table.getChildren().get(0).getAttr();
		List<Node> colDetails = node.get(1).getChildren();
		for (Node nextNode : colDetails) {
			assert nextNode.getAttr().equalsIgnoreCase("create_attr") : "Invalid sql: cannot find create_attr node.";
			//List<Node> attrNodes = nextNode.getChildren().get(0).getChildren();
			fieldName.add(nextNode.getChildren().get(0).getChildren().get(0).getAttr());
			String type = nextNode.getChildren().get(1).getChildren().get(0).getAttr();
			if (type.equalsIgnoreCase("INT")) {
				fieldType.add(FieldType.INT);
			} else if (type.equalsIgnoreCase("STR20")) {
				fieldType.add(FieldType.STR20);
			} else {
				System.out.println("Invalid sql: invalid attribute type!");
				return;
			}
		}
		Schema schema = new Schema(fieldName, fieldType);
		Relation newRelation = schemaManager.createRelation(relationName, schema);

		// handle error
		if (newRelation != null) {
			System.out.println("CREATE: successfully created relation " + relationName);
		} else {
			System.out.println("CREATE: failed to creat relation " + relationName);
		}
		return;
	}

	private void insertQuery(List<Node> node) {

		List<String> colNameList;
		ArrayList<String> value_list = new ArrayList<String>();
		String relationName = null;
		List<Node> colList = null;

		for (Node subNode : node) {
			String name = subNode.getAttr();
			if (name.equalsIgnoreCase("TABLE")) {
				relationName = subNode.getChildren().get(0).getAttr();
			} else if (name.equalsIgnoreCase("ATTR_LIST")) {
				colList = subNode.getChildren();
			} else if (name.equalsIgnoreCase("VALUES")) {
				Relation relation = schemaManager.getRelation(relationName);
				Tuple newTuple = relation.createTuple();
				assert colList != null : "ERROE: column list is null";

				int index = 0;
				for (Node attrNode : colList) {
					assert attrNode.getAttr().equalsIgnoreCase("ATTR_NAME") : "ERROR: not ATTR_NAME node";
					assert subNode.getChildren().get(index).getAttr()
							.equalsIgnoreCase("VALUE") : "ERROR: not VALUE node";
					assert attrNode.getChildren().size() == 1 : "ERROR: ATTR_NAME list length not 1";
					assert newTuple.getSchema().getFieldType(
							attrNode.getChildren().get(0).getAttr()) != null : "ERROR: cannot get attr type";

					String value = subNode.getChildren().get(index).getChildren().get(0).getAttr();
					String type = attrNode.getChildren().get(0).getAttr();
					if (newTuple.getSchema().getFieldType(type).equals(FieldType.INT)) {
						if(value.equalsIgnoreCase("NULL")) {
							newTuple.setField(type, Integer.MIN_VALUE);
						}else {
							newTuple.setField(type, Integer.parseInt(value));
						}
						
					} else {
						newTuple.setField(type, value);
					}
					index += 1;
				}
				appendTupleToRelation(relation, memory, 0, newTuple);
			} else if (name.equalsIgnoreCase("SELECT")) {
				SelectPrinter sp = new SelectPrinter(subNode.getChildren(), memory, disk, schemaManager);
				sp.insertSelectRelation(relationName, colList);
			}
		}
		System.out.println("INSERT COMPLETE");

	}

	private void appendTupleToRelation(Relation relation, MainMemory memory, int i, Tuple tuple) {
		Block block;
		if (relation.getNumOfBlocks() == 0) {
			block = memory.getBlock(i);
			block.clear(); // clear the block
			block.appendTuple(tuple); // append the tuple
			relation.setBlock(relation.getNumOfBlocks(), i);
		} else {
			relation.getBlock(relation.getNumOfBlocks() - 1, i);
			block = memory.getBlock(i);
			if (block.isFull()) {
				block.clear(); // clear the block
				block.appendTuple(tuple); // append the tuple
				relation.setBlock(relation.getNumOfBlocks(), i);

			} else {
				block.appendTuple(tuple); // append the tuple
				relation.setBlock(relation.getNumOfBlocks() - 1, i);
			}
		}
	}

	private void selectQuery(List<Node> nodes) throws IOException {

		SelectPrinter sp = new SelectPrinter(nodes, memory, disk, schemaManager);
		sp.runSelect();
	}

}
