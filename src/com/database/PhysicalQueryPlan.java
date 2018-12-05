package com.database;

import java.util.ArrayList;
import java.util.PriorityQueue;
import storageManager.*;

public class PhysicalQueryPlan {
	public static Relation crossJoin(SchemaManager schemaMan, MainMemory mem, String tableA, String tableB) {
		Relation productOfAB;
		ArrayList<Tuple> tuples;

		Relation relationA = schemaMan.getRelation(tableA);
		Relation relationB = schemaMan.getRelation(tableB);

		// create the schema of the product from the two relations doing cross join
		Schema combinedSchema = mergeSchema(relationA, relationB);

		String productTableName = tableA + "_cross_" + tableB;
		if (schemaMan.relationExists(productTableName)) {
			schemaMan.deleteRelation(productTableName);
		}
		productOfAB = schemaMan.createRelation(productTableName, combinedSchema);

		int smallerSize = Math.min(relationA.getNumOfBlocks(), relationB.getNumOfBlocks());
		if (smallerSize < mem.getMemorySize() - 1) {
			// choose one-pass algorithm
			tuples = onePassCrossJoin(schemaMan, mem, relationA, relationB, productOfAB);
		} else {
			// choose nested-loop algorithm
			tuples = nestedLoopCrossJoin(schemaMan, mem, relationA, relationB, productOfAB);
		}

		// save tuples to last one block in memory and then save the block to disk
		int tuples_per_block = tuples.get(0).getTuplesPerBlock();
		int num_blocks = tuples.size() / tuples_per_block;
		for (int i = 0; i < num_blocks; i++) {
			mem.setTuples(mem.getMemorySize() - 1,
					new ArrayList<Tuple>(tuples.subList(i * tuples_per_block, (i + 1) * tuples_per_block)));
			productOfAB.setBlock(i, mem.getMemorySize() - 1);
		}
		// handle the last few tuples that do not fill up one block if they exist
		if (tuples.size() % tuples_per_block > 0) {
			mem.setTuples(mem.getMemorySize() - 1,
					new ArrayList<Tuple>(tuples.subList(num_blocks * tuples_per_block, tuples.size())));
			productOfAB.setBlock(num_blocks, mem.getMemorySize() - 1);
		}

		return productOfAB;
	}

	// handle cross join of multiple tables
	public static Relation crossJoin(SchemaManager schema_Man, MainMemory mem, String... tables) {
		Relation product = null;
		String intermediate = tables[0];
		for (int i = 1; i < tables.length; i++) {
			product = crossJoin(schema_Man, mem, tables[i], intermediate);
			intermediate = product.getRelationName();
		}
		return product;
	}

	private static ArrayList<Tuple> onePassCrossJoin(SchemaManager schemaMan, MainMemory mem, Relation relationA, Relation relationB, Relation product) {
		ArrayList<Tuple> results = new ArrayList<Tuple>();
		Relation small, large;

		if (relationA.getNumOfBlocks() < relationB.getNumOfBlocks()) {
			small = relationA;
			large = relationB;
		} else {
			small = relationB;
			large = relationA;
		}

		int smallNumOfBlocks = small.getNumOfBlocks();
		int largeNumOfBlocks = large.getNumOfBlocks();

		// read all blocks of the small relation in one pass to the memory and access
		// its tuples
		small.getBlocks(0, 0, smallNumOfBlocks);
		ArrayList<Tuple> tuplesSmall = mem.getTuples(0, smallNumOfBlocks);

		// read large relation one by one block to memory and do cross join operation
		for (int i = 0; i < largeNumOfBlocks; i++) {

			large.getBlock(i, smallNumOfBlocks); // use the memory block next to the end of blocks storing small
													// relation
			Block blockLarge = mem.getBlock(smallNumOfBlocks);
			ArrayList<Tuple> tuplesLarge = blockLarge.getTuples();

			// do Cartesian product
			for (Tuple lTuple : tuplesLarge) {
				for (Tuple sTuple : tuplesSmall) {
					// make sure columns of tableA appears to the left of columns of tableB
					if (small == relationA) {
						results.add(joinTwoTuples(sTuple, lTuple, product));
					} else {
						results.add(joinTwoTuples(lTuple, sTuple, product));
					}
				}
			}
		}
		return results;
	}

	private static ArrayList<Tuple> nestedLoopCrossJoin(SchemaManager schemaMan, MainMemory mem, Relation relationA, Relation relationB, Relation product) {
		ArrayList<Tuple> results = new ArrayList<Tuple>();
		Relation small, large;

		if (relationA.getNumOfBlocks() < relationB.getNumOfBlocks()) {
			small = relationA;
			large = relationB;
		} else {
			small = relationB;
			large = relationA;
		}

		int smallNumOfBlocks = small.getNumOfBlocks();
		int largeNumOfBlocks = large.getNumOfBlocks();

		// because smallNumOfBlocks >= M, has to do multiple batches to read all blocks of small
		// each batch has M-2 number of blocks except the last batch
		int MEM_SIZE = mem.getMemorySize();
		int batchNum = smallNumOfBlocks / (MEM_SIZE - 2) + 1;

		// read M-1 blocks of small into memory each batch
		// outer loop
		for (int i = 0; i < batchNum; i++) {

			int num_blocks = (i == batchNum - 1) ? smallNumOfBlocks - (batchNum - 1) * (MEM_SIZE - 2) : MEM_SIZE - 2;
			small.getBlocks(i * (MEM_SIZE - 2), 0, num_blocks);
			ArrayList<Tuple> tuplesSmall = mem.getTuples(0, num_blocks);

			// read large relation one by one block to memory and do cross join operation
			// inner loop
			for (int j = 0; j < largeNumOfBlocks; j++) {

				large.getBlock(j, MEM_SIZE - 2); // read one block of large
				Block blockLarge = mem.getBlock(MEM_SIZE - 2);
				ArrayList<Tuple> tuplesLarge = blockLarge.getTuples();

				// do Cartesian product
				for (Tuple lTuple : tuplesLarge) {
					for (Tuple sTuple : tuplesSmall) {
						// make sure columns of tableA appears to the left of columns of tableB
						if (small == relationA) {
							results.add(joinTwoTuples(sTuple, lTuple, product));
						} else {
							results.add(joinTwoTuples(lTuple, sTuple, product));
						}
					}
				}
			}
		}
		return results;
	}

	private static Schema mergeSchema(Relation a, Relation b) {
		ArrayList<String> fieldsMerged = new ArrayList<String>();
		ArrayList<FieldType> typesMerged = new ArrayList<FieldType>();
		Schema schemaA = a.getSchema();
		Schema schemaB = b.getSchema();

		// get fieldNames and fieldTypes of two relations
		ArrayList<String> fieldsA = schemaA.getFieldNames();
		ArrayList<FieldType> typesA = schemaA.getFieldTypes();
		ArrayList<String> fieldsB = schemaB.getFieldNames();
		ArrayList<FieldType> typesB = schemaB.getFieldTypes();

		// prepend table name to each table's field names
		String tableA = a.getRelationName();
		String tableB = b.getRelationName();

		for (String fieldName : fieldsA) {
			if (!fieldName.contains(".")) {
				fieldName = tableA + "." + fieldName;
			}
			fieldsMerged.add(fieldName);
		}
		for (String fieldName : fieldsB) {
			if (!fieldName.contains(".")) {
				fieldName = tableB + "." + fieldName;
			}
			fieldsMerged.add(fieldName);
		}

		typesMerged.addAll(typesA);
		typesMerged.addAll(typesB);

		return new Schema(fieldsMerged, typesMerged);
	}

	private static Tuple joinTwoTuples(Tuple a, Tuple b, Relation product) {
		int numOfFieldsA = a.getNumOfFields();
		int numOfFieldsB = b.getNumOfFields();
		Tuple merged = product.createTuple();
		for (int i = 0; i < numOfFieldsA; i++) {
			if (a.getField(i).type == FieldType.INT) {
				merged.setField(i, a.getField(i).integer);
			} else {
				merged.setField(i, a.getField(i).str);
			}
		}
		for (int j = 0; j < numOfFieldsB; j++) {
			if (b.getField(j).type == FieldType.INT) {
				merged.setField(numOfFieldsA + j, b.getField(j).integer);
			} else {
				merged.setField(numOfFieldsA + j, b.getField(j).str);
			}
		}
		return merged;
	}

	public static Relation naturalJoin(SchemaManager schemaMan, MainMemory mem, String tableA, String tableB, String commonAttr) {
		Relation naturalJoinRelation;
		ArrayList<Tuple> tuples;
		
		Relation relationA = schemaMan.getRelation(tableA);
		Relation relationB = schemaMan.getRelation(tableB);
		
		Schema mergedSchema = mergeSchema(relationA, relationB);
		String joinTableName = tableA + "_join_" + tableB;
		if(schemaMan.relationExists(joinTableName)) {
			schemaMan.deleteRelation(joinTableName);
		}
		naturalJoinRelation = schemaMan.createRelation(joinTableName, mergedSchema);
		
		int smallerSize = Math.min(relationA.getNumOfBlocks(), relationB.getNumOfBlocks());
		if(smallerSize < mem.getMemorySize()) {
			tuples = onePassNaturalJoin(schemaMan, mem, relationA, relationB, naturalJoinRelation, commonAttr);
		}
		else {
			tuples = twoPassSortMerge(schemaMan, mem, relationA, relationB, naturalJoinRelation, commonAttr);
		}
		
		// save tuples to last one block in memory and then save the block to disk
		int tuples_per_block = tuples.get(0).getTuplesPerBlock();
		int num_blocks = tuples.size() / tuples_per_block;
		for (int i = 0; i < num_blocks; i++) {
			mem.setTuples(mem.getMemorySize() - 1,
					new ArrayList<Tuple>(tuples.subList(i * tuples_per_block, (i + 1) * tuples_per_block)));
			naturalJoinRelation.setBlock(i, mem.getMemorySize() - 1);
		}
		// handle the last few tuples that do not fill up one block if they exist
		if (tuples.size() % tuples_per_block > 0) {
			mem.setTuples(mem.getMemorySize() - 1,
					new ArrayList<Tuple>(tuples.subList(num_blocks * tuples_per_block, tuples.size())));
			naturalJoinRelation.setBlock(num_blocks, mem.getMemorySize() - 1);
		}

		return naturalJoinRelation;
	}
	
	private static ArrayList<Tuple> onePassNaturalJoin(SchemaManager schemaMan, MainMemory mem, Relation relationA, Relation relationB, Relation product, String commonAttr) {
		ArrayList<Tuple> results = new ArrayList<Tuple>();
		Relation small, large;

		if (relationA.getNumOfBlocks() < relationB.getNumOfBlocks()) {
			small = relationA;
			large = relationB;
		} else {
			small = relationB;
			large = relationA;
		}

		int smallNumOfBlocks = small.getNumOfBlocks();
		int largeNumOfBlocks = large.getNumOfBlocks();

		// read all blocks of the small relation in one pass to the memory and access its tuples
		small.getBlocks(0, 0, smallNumOfBlocks);
		ArrayList<Tuple> tuplesSmall = mem.getTuples(0, smallNumOfBlocks);
		
		// read large relation one by one block to memory and do cross join operation
		for (int i = 0; i < largeNumOfBlocks; i++) {

			large.getBlock(i, smallNumOfBlocks); // use the memory block next to the end of blocks storing small relation
			Block blockLarge = mem.getBlock(smallNumOfBlocks);
			ArrayList<Tuple> tuplesLarge = blockLarge.getTuples();

			// do natural join
			for (Tuple lTuple : tuplesLarge) {
				for (Tuple sTuple : tuplesSmall) {
					
					// check equi-join condition
					if (sTuple.getField(commonAttr).type.equals(lTuple.getField(commonAttr).type) && sTuple.getField(commonAttr).toString().equals(lTuple.getField(commonAttr).toString())) {
						// make sure columns of tableA appears to the left of columns of tableB
						if (small == relationA) {
							results.add(joinTwoTuples(sTuple, lTuple, product));
						} else {
							results.add(joinTwoTuples(lTuple, sTuple, product));
						}
					}
				}
			}
		}
		return results;
	}
	
	private static ArrayList<Tuple> twoPassSortMerge(SchemaManager schemaMan, MainMemory mem, Relation relationA, Relation relationB, Relation product, String commonAttr) {
		ArrayList<Tuple> results = new ArrayList<Tuple>();		
		TupleComparator comp = new TupleComparator(commonAttr);

		int numOfBlocksA = relationA.getNumOfBlocks();
		int numOfBlocksB = relationB.getNumOfBlocks();
		int memSize = mem.getMemorySize();
		
		// estimate number of sublists each relation will be divided into, M >= sqrt(B(R) + B(S))
		int listNumA = numOfBlocksA * memSize / (numOfBlocksA + numOfBlocksB) + 1;
		int listNumB = numOfBlocksB * memSize / (numOfBlocksA + numOfBlocksB) + 1;
		int listSize = (numOfBlocksA % listNumA == 0) ? numOfBlocksA / listNumA : numOfBlocksA / listNumA + 1;
		
		//phase 1 of two-pass: sort tuples of each relation into sublists
		ArrayList<Pair<Integer, Integer>> blockIndexOfSublistsA = createSublists(mem, relationA, comp, listNumA);
		ArrayList<Pair<Integer, Integer>> blockIndexOfSublistsB = createSublists(mem, relationB, comp, listNumB);
		
		
		for (int i = 0; i < listSize; i++) {
			for (int j = 0; j < listNumA; j++) {
				relationA.getBlock(blockIndexOfSublistsA.get(j).first + i, j);
				
			}
			for (int j = 0; j < listNumB; j++) {
				relationB.getBlock(blockIndexOfSublistsB.get(j).first + i, listNumA + j);
			}
			
		}
		
		return results;
		
		
	}
	
	private static ArrayList<Pair<Integer, Integer>> createSublists(MainMemory mem, Relation relation, TupleComparator comp, int listNum) {
		ArrayList<Tuple> temp = new ArrayList<Tuple>();
		ArrayList<Pair<Integer, Integer>> blockIndexOfSublists = new ArrayList<>();
		int numOfBlocks = relation.getNumOfBlocks();
		int tuples_per_block = relation.getSchema().getTuplesPerBlock();
		int listSize = (numOfBlocks % listNum == 0) ? numOfBlocks / listNum : numOfBlocks / listNum + 1;

		int lastListSize = numOfBlocks - listSize * (listNum - 1); // the size of last list may be smaller than the average size
		
		//divide all tuples of relation into listNum lists, each list with listSize of blocks except last batch
		//read each batch into memory, sort tuples in a priority queue, write them back into disk
		PriorityQueue<Tuple> sortedList = new PriorityQueue<Tuple>(lastListSize * tuples_per_block, comp);
		
		for (int i = 0; i < listNum; i++) {
			//read listSize of blocks into memory, handle the exception of last list
			int numOfReadBlocks = (i == listNum - 1) ? lastListSize : listSize;
			blockIndexOfSublists.add(new Pair<>(i * listSize, i * listSize + numOfReadBlocks));
			
			relation.getBlocks(i * listSize, 0, numOfReadBlocks);
			
			//add all tuples in the memory to sortedList
			ArrayList<Tuple> tuplesInMem = mem.getTuples(0, numOfReadBlocks);
			for(Tuple t : tuplesInMem) {
				sortedList.add(t);
			}
			
			//use sorted tuples to set memory block and then read into disk
			for (int j = 0; j < numOfReadBlocks; j++) {
				temp.clear();
				for (int k = 0; k < tuples_per_block; k++) {
					if (sortedList.size() != 0) {
						temp.add(sortedList.remove());
					}
					else {
						break;
					}
				}
				mem.setTuples(j, temp);
				relation.setBlock(j + i * listSize, j);
			}
			sortedList.clear();
		}
		return blockIndexOfSublists;
	}
	

}











