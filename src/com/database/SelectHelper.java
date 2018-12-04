package com.database;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import com.database.entity.Node;

import storageManager.Block;
import storageManager.FieldType;
import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.Schema;
import storageManager.SchemaManager;
import storageManager.Tuple;

public class SelectHelper {

	public static Relation distinct(SchemaManager schemaManager, Relation relation, MainMemory memory,
			ArrayList<String> fieldList) {
		// for(String fieldName : fieldList) {}
		String fieldName = fieldList.get(0);
		String name = relation.getRelationName() + "_distinct_" + fieldName;
		if (schemaManager.relationExists(name))
			schemaManager.deleteRelation(name);
		ArrayList<Tuple> tuples;
		if (relation.getNumOfBlocks() <= memory.getMemorySize()) {
			tuples = onePassRemoveDuplicate(relation, memory, fieldName);
		} else {
			tuples = twoPassRemoveDuplicate(relation, memory, fieldName);
		}
		return createRelationFromTuples(tuples, name, schemaManager, relation, memory);
	}

	private static ArrayList<Tuple> twoPassRemoveDuplicate(Relation relation, MainMemory memory, String fieldName) {
		// phase 1: making sorted sublists
		twoPassHelper(relation, memory, fieldName);

		// phase 2
		int numOfBlocks = relation.getNumOfBlocks();
		HashSet<String> hashSet = new HashSet<>();
		ArrayList<Tuple> res = new ArrayList<>();
		ArrayList<ArrayList<Tuple>> tuples = new ArrayList<>();
		ArrayList<Pair<Integer, Integer>> blockIndexOfSublists = new ArrayList<>();

		// bring in a block from each of the sorted sublists
		for (int i = 0, j = 0; i < numOfBlocks; i += memory.getMemorySize(), j++) {
			// initial index must be i + 1
			blockIndexOfSublists.add(new Pair<>(i + 1, Math.min(i + memory.getMemorySize(), numOfBlocks)));
			relation.getBlock(i, j);
			tuples.add(memory.getTuples(j, 1));
		}

		for (int k = 0; k < relation.getNumOfTuples(); ++k) {
			for (int i = 0; i < blockIndexOfSublists.size(); ++i) {
				// read in the next block form a sublist if its block is exhausted
				if (tuples.get(i).isEmpty()
						&& (blockIndexOfSublists.get(i).first < blockIndexOfSublists.get(i).second)) {
					relation.getBlock(blockIndexOfSublists.get(i).first, i);
					tuples.set(i, memory.getTuples(i, 1));
					blockIndexOfSublists.get(i).first++;
				}
			}

			// find the smallest key among the first remaining elements of all the sublists
			ArrayList<Tuple> minTuples = new ArrayList<>();
			for (int j = 0; j < tuples.size(); ++j) {
				if (!tuples.isEmpty() && !tuples.get(j).isEmpty())
					minTuples.add(tuples.get(j).get(0));
			}

			// the first difference to twoPassSort -
			// multiple elements could be removed in one loop, the number of loops could be
			// less than numOfBlocks
			// so the loop could break earlier
			if (minTuples.isEmpty())
				break;

			Tuple minTuple = Collections.min(minTuples, new TupleComparator(fieldName));
			// the 2nd difference - use Hashset
			if (minTuple.getField(fieldName).type.equals(FieldType.STR20)) {
				if (hashSet.add(minTuple.getField(fieldName).str))
					res.add(minTuple);
			} else {
				if (hashSet.add(Integer.toString(minTuple.getField(fieldName).integer)))
					res.add(minTuple);
			}

			// the 3rd difference - remove all minimum elements
			for (ArrayList<Tuple> tuple : tuples) {
				if (!tuple.isEmpty()) {
					if (tuple.get(0).getField(fieldName).type.equals(minTuple.getField(fieldName).type)) {
						if (tuple.get(0).getField(fieldName).type.equals(FieldType.STR20)) {
							if (tuple.get(0).getField(fieldName).str.equals(minTuple.getField(fieldName).str))
								tuple.remove(0);
						} else {
							if (tuple.get(0).getField(fieldName).integer == minTuple.getField(fieldName).integer)
								tuple.remove(0);
						}
					}
				}
			}
//            for(int j = 0; j < tuples.size(); ++j){
//                if(!tuples.get(j).isEmpty()) {
//                    if(tuples.get(j).get(0).getField(fieldName).type.equals(minTuple.getField(fieldName).type)){
//                        if(tuples.get(j).get(0).getField(fieldName).type.equals(FieldType.STR20)){
//                            if(tuples.get(j).get(0).getField(fieldName).str.equals(minTuple.getField(fieldName).str))
//                                tuples.get(j).remove(0);
//                        }else{
//                            if(tuples.get(j).get(0).getField(fieldName).integer == minTuple.getField(fieldName).integer)
//                                tuples.get(j).remove(0);
//                        }
//                    }
//                }
//            }
		}

		// for(Tuple tuple : res) System.out.println(tuple);
		clearMemory(memory);
		return res;
	}

	private static ArrayList<Tuple> onePassRemoveDuplicate(Relation relation, MainMemory memory, String fieldName) {
		ArrayList<Tuple> res = new ArrayList<>();
		HashSet<String> hashSet = new HashSet<>();
		int numOfBlocks = relation.getNumOfBlocks();
		relation.setBlocks(0, 0, numOfBlocks);
		ArrayList<Tuple> tuples = memory.getTuples(0, numOfBlocks);
		for (Tuple tuple : tuples) {
			if (tuple.getField(fieldName).type.equals(FieldType.STR20)) {
				if (hashSet.add(tuple.getField(fieldName).str))
					res.add(tuple);
			} else {
				if (hashSet.add(Integer.toString(tuple.getField(fieldName).integer)))
					res.add(tuple);
			}
		}
		clearMemory(memory);
		return res;
	}

	public static void clearMemory(MainMemory memory) {
		for (int i = 0; i < memory.getMemorySize(); ++i)
			memory.getBlock(i).clear();
	}

	public static Relation select(SchemaManager schemaManager, Relation relation, MainMemory memory, Node col,
			Node order, Expression expression) {
		ArrayList<Tuple> tuples = new ArrayList<>();
		int numOfBlocks = relation.getNumOfBlocks(), memoryBlocks = memory.getMemorySize();

		if (numOfBlocks <= memoryBlocks) {
			tuples = selectQueryHelper(expression, relation, memory, 0, numOfBlocks);
		} else {
			int remainNumber = numOfBlocks;
			int relationindex = 0;
			ArrayList<Tuple> tmp;
			while (remainNumber > memoryBlocks) {
				tmp = selectQueryHelper(expression, relation, memory, relationindex, memoryBlocks);
				tuples.addAll(tmp);
				remainNumber = remainNumber - memoryBlocks;
				relationindex = relationindex + memoryBlocks;
			}
			tmp = selectQueryHelper(expression, relation, memory, relationindex, remainNumber);
			tuples.addAll(tmp);
		}
		String name = relation.getRelationName() + "_select_";
		if (schemaManager.relationExists(name))
			schemaManager.deleteRelation(name);
		return createRelationFromTuples(tuples, name, schemaManager, relation, memory);
	}

	private static ArrayList<Tuple> selectQueryHelper(Expression expression, Relation relation, MainMemory memory,
			int relationIndex, int numOfBlocks) {
		Block block;
		ArrayList<Tuple> res = new ArrayList<>();
		relation.getBlocks(relationIndex, 0, numOfBlocks);
		for (int i = 0; i < numOfBlocks; i++) {
			block = memory.getBlock(i);
			ArrayList<Tuple> tuples = block.getTuples();
			for (Tuple tuple : tuples) {
				if (expression != null) {
					if (expression.evaluateOperation(tuple))
						res.add(tuple);
				} else {
					// System.out.println(tuple);
					res.add(tuple);
				}
			}
		}
		return res;
	}

	public static Relation sort(SchemaManager schemaManager, Relation relation, MainMemory memory, Node col, Node order,
			Expression expression) {
		String fieldName = order.getChildren().get(0).getChildren().get(0).getAttr();
		String name = relation.getRelationName() + "_sortBy_" + fieldName;
		if (schemaManager.relationExists(name))
			schemaManager.deleteRelation(name);
		ArrayList<Tuple> tuples;
		if (relation.getNumOfBlocks() <= memory.getMemorySize()) {
			tuples = onePassSort(relation, memory, fieldName);
		} else {
			tuples = twoPassSort(relation, memory, fieldName);
		}
		return createRelationFromTuples(tuples, name, schemaManager, relation, memory);
	}

	private static ArrayList<Tuple> twoPassSort(Relation relation, MainMemory memory, String fieldName) {
		// phase 1: making sorted sublists
		twoPassHelper(relation, memory, fieldName);

		// phase 2: merging
		int numOfBlocks = relation.getNumOfBlocks();
		ArrayList<Tuple> res = new ArrayList<>();
		ArrayList<ArrayList<Tuple>> tuples = new ArrayList<>();
		ArrayList<Pair<Integer, Integer>> blockIndexOfSublists = new ArrayList<>();

		// bring in a block from each of the sorted sublists
		for (int i = 0, j = 0; i < numOfBlocks; i += memory.getMemorySize(), j++) {
			// initial index must be i + 1
			blockIndexOfSublists.add(new Pair<>(i + 1, Math.min(i + memory.getMemorySize(), numOfBlocks)));
			relation.getBlock(i, j);
			tuples.add(memory.getTuples(j, 1));
		}

		for (int k = 0; k < relation.getNumOfTuples(); ++k) {
			for (int i = 0; i < blockIndexOfSublists.size(); ++i) {
				// read in the next block from a sublist if its block is exhausted
				if (tuples.get(i).isEmpty()
						&& (blockIndexOfSublists.get(i).first < blockIndexOfSublists.get(i).second)) {
					relation.getBlock(blockIndexOfSublists.get(i).first, i);
					tuples.set(i, memory.getTuples(i, 1));
					blockIndexOfSublists.get(i).first++;
				}
			}

			// find the smallest key among the first remaining elements of all the sublists
			ArrayList<Tuple> minTuples = new ArrayList<>();
			for (int j = 0; j < tuples.size(); ++j) {
				if (!tuples.isEmpty() && !tuples.get(j).isEmpty())
					minTuples.add(tuples.get(j).get(0));
			}
			Tuple minTuple = Collections.min(minTuples, new TupleComparator(fieldName));
			res.add(minTuple);

			// remove the minimum element
			for (int j = 0; j < tuples.size(); ++j) {
				if (!tuples.get(j).isEmpty() && tuples.get(j).get(0).equals(minTuple))
					tuples.get(j).remove(0);
			}
		}

		// for(Tuple tuple : res) System.out.println(tuple);
		clearMemory(memory);
		return res;
	}

	private static void twoPassHelper(Relation relation, MainMemory memory, String fieldName) {
		int numOfBlocks = relation.getNumOfBlocks(), sortedBlocks = 0;
		ArrayList<Tuple> tuples;
		while (sortedBlocks < numOfBlocks) {
			int t = Math.min(memory.getMemorySize(), numOfBlocks - sortedBlocks);
			relation.getBlocks(sortedBlocks, 0, t);
			// sort main memory
			tuples = onePassSort(memory, fieldName, t);
			memory.setTuples(0, tuples);
			relation.setBlocks(sortedBlocks, 0, t);
			// t <= memory.getMemorySize() ---> error!!!!(When numOfBlocks > 10)
			if (t < memory.getMemorySize()) {
				break;
			} else {
				sortedBlocks += memory.getMemorySize();
			}
			clearMemory(memory);
		}
	}

	private static ArrayList<Tuple> onePassSort(MainMemory memory, String fieldName, int t) {
		ArrayList<Tuple> tuples = memory.getTuples(0, t);
		tuples.sort(new TupleComparator(fieldName));
		clearMemory(memory);
		return tuples;
	}

	private static Relation createRelationFromTuples(ArrayList<Tuple> tuples, String name, SchemaManager schemaManager,
			Relation relation, MainMemory memory) {
		Schema schema = relation.getSchema();
		if (schemaManager.relationExists(name))
			schemaManager.deleteRelation(name);
		Relation tempRelation = schemaManager.createRelation(name, schema);
		int tupleNumber = tuples.size(), tuplesPerBlock = schema.getTuplesPerBlock();
		int tupleBlocks;
		if (tupleNumber < tuplesPerBlock) {
			tupleBlocks = 1;
		} else if (tupleNumber >= tuplesPerBlock && tupleNumber % tuplesPerBlock == 0) {
			tupleBlocks = tupleNumber / tuplesPerBlock;
		} else {
			tupleBlocks = tupleNumber / tuplesPerBlock + 1;
		}

		int index = 0;
		while (index < tupleBlocks) {
			int t = Math.min(memory.getMemorySize(), tupleBlocks - index);
			for (int i = 0; i < t; i++) {
				Block block = memory.getBlock(i);
				block.clear();
				for (int j = 0; j < tuplesPerBlock; j++) {
					if (!tuples.isEmpty()) {
						Tuple temp = tuples.get(0);
						block.setTuple(j, temp);
						tuples.remove(temp);
					} else {
						break;
					}
				}
			}
			tempRelation.setBlocks(index, 0, t);
			if (t < memory.getMemorySize()) {
				break;
			} else {
				index += memory.getMemorySize();
			}
		}
		return tempRelation;
	}

	// sorting relation by certain field
	private static ArrayList<Tuple> onePassSort(Relation relation, MainMemory memory, String fieldName) {
		int numOfBlocks = relation.getNumOfBlocks();
		relation.getBlocks(0, 0, numOfBlocks);
		ArrayList<Tuple> tuples = memory.getTuples(0, numOfBlocks);
		tuples.sort(new TupleComparator(fieldName));
		clearMemory(memory);
		return tuples;
	}

	public static void project(Relation relation, MainMemory memory, Node col, Expression expression, BufferedWriter bw)
			throws IOException {
		int numOfBlocks = relation.getNumOfBlocks();
		int i = 0;
		System.out.println(relation);
		System.out.println(numOfBlocks);
		while (i < numOfBlocks) {
			// System.out.println("here!!!");
			int t = Math.min(memory.getMemorySize(), numOfBlocks - i);
			relation.getBlocks(i, 0, t);
			if (memory.getBlock(0).isEmpty()) {
				System.out.println("No Selected Tuples");
				return;
			}
			projectHelper(relation, memory, col, t, bw);
			if (t <= memory.getMemorySize())
				break;
			else
				i += 10;
			System.out.println(i);
		}
	}

	@SuppressWarnings("unlikely-arg-type")
	private static void projectHelper(Relation relation, MainMemory memory, Node col, int numOfBlocks,
			BufferedWriter bw) throws IOException {
		ArrayList<Tuple> tuples = memory.getTuples(0, numOfBlocks);
		Node mutiNode = col.getChildren().get(0).getChildren().get(0);
		String distinctAttr = col.getChildren().get(0).getAttr();
		List<Node> dictinctNodes = col.getChildren().get(0).getChildren();
		if (mutiNode.equals("*")) {
			printMuti(tuples, bw);
		} else {
			if (distinctAttr.equalsIgnoreCase("DISTINCT")) {
				printField(bw, col.getChildren().get(0).getChildren());
			} else {
				printField(bw, col.getChildren());
			}
		}
		System.out.println();
		bw.write("\n");
		for (Tuple t : tuples) {
			if (distinctAttr.equalsIgnoreCase("DISTINCT")) { // has distinct
				printValuesFromTuples(bw, dictinctNodes, t);
			} else { // without distinct
				printValuesFromTuples(bw, col.getChildren(), t);
			}
			System.out.println();
			bw.write("\n");
		}
		System.out.println("--------------------------------");
		bw.write("--------------------------------\n\n\n");

	}

	private static void printMuti(ArrayList<Tuple> tuples, BufferedWriter bw) throws IOException {
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

	private static void printField(BufferedWriter bw, List<Node> nodes) throws IOException {
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

	private static void printValuesFromTuples(BufferedWriter bw, List<Node> distinctNodes, Tuple t) throws IOException {
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

	public static void projectInsert(Relation relation, MainMemory memory, List<Node> colList, Expression expression) {
		int numOfBlocks = relation.getNumOfBlocks();
		int i = 0;
		System.out.println(relation);
		System.out.println(numOfBlocks);
		while (i < numOfBlocks) {
			// System.out.println("here!!!");
			int t = Math.min(memory.getMemorySize(), numOfBlocks - i);
			relation.getBlocks(i, 0, t);
			if (memory.getBlock(0).isEmpty()) {
				System.out.println("No Selected Tuples");
				return;
			}
			projectInsertHelper(relation, memory, colList, t);
			if (t <= memory.getMemorySize())
				break;
			else
				i += 10;
			System.out.println(i);
		}

	}

	private static void projectInsertHelper(Relation relation, MainMemory memory, List<Node> colList, int numOfBlocks) {
		// TODO Auto-generated method stub
		ArrayList<Tuple> tuples = memory.getTuples(0, numOfBlocks);
		Tuple newTuple = relation.createTuple();
		ArrayList<Tuple> output = new ArrayList<>();
		for (Tuple t : tuples) {
			for (int j = 0; j < colList.size(); j++) {
				if (t.getField(colList.get(j).getAttr()).type == FieldType.INT)
					newTuple.setField(j, Integer.parseInt(t.getField(colList.get(j).getAttr()).toString()));
				else
					newTuple.setField(j, t.getField(colList.get(j).getAttr()).toString());
			}

			output.add(newTuple);
			newTuple = relation.createTuple();
		}
		int blk_cnt = relation.getNumOfBlocks();
		Block blk = memory.getBlock(0);
		ArrayList<FieldType> fieldTypes = new ArrayList<>();
		for (Node s : colList) {
			fieldTypes.add(relation.getSchema().getFieldType(s.getAttr()));
		}
		ArrayList<String> fields = new ArrayList<>();
		for(Node col : colList) {
			fields.add(col.getAttr());
		}
		Schema schema = new Schema(fields, fieldTypes);
		while (!output.isEmpty()) {
			blk.clear();
			for (int i = 0; i < schema.getTuplesPerBlock(); i++) {
				if (!output.isEmpty()) {
					Tuple t = output.get(0);
					blk.setTuple(i, t);
					output.remove(t);
				}
			}
			relation.setBlock(blk_cnt++, 0);
		}
	}

}

class Pair<A, B> {
	public A first;
	public B second;

	public Pair() {
	};

	public Pair(A first, B second) {
		this.first = first;
		this.second = second;
	}
}
