package com.database;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.SchemaManager;

public class Helper {

	public static Relation executeNaturalJoin(SchemaManager schemaManager, MainMemory memory, String table1,
			String table2, String field0, int i) {
		// TODO Auto-generated method stub
		return null;
	}

	public static Relation filter(SchemaManager schemaManager, MainMemory memory, Relation r, Expression expression,
			ArrayList<String> fields, int i) {
		// TODO Auto-generated method stub
		return null;
		
	}

	public static Relation executeDistinct(SchemaManager schemaManager, MainMemory memory, Relation ra,
			ArrayList<String> fields, int i) {
		// TODO Auto-generated method stub
		return null;
		
	}

	public static void executeOrder(SchemaManager schemaManager, MainMemory memory, Relation ra,
			ArrayList<String> orderField, int i) {
		// TODO Auto-generated method stub
		
	}

	public static List<String> fileReader(File file) {
		 BufferedReader reader = null;
		    try{
		      reader = new BufferedReader(new FileReader(file));
		      List<String> inputLines=new ArrayList<String>();
		      String cmd;
		      while((cmd=reader.readLine())!=null){
		        inputLines.add(cmd);
		      }
		      reader.close();
		      return inputLines;
		    }catch(IOException e){
		      e.printStackTrace();
		      return null;
		    }finally {
		      if(reader!=null)
		        try{
		          reader.close();
		        }catch (IOException e) {
		          e.printStackTrace();
		        }
		}
	}


}
