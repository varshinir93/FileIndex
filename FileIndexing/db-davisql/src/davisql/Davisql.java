package davisql;

import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
//go to putvalues
public class Davisql {
	static String prompt ="davisql> ";
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	public static String userCommand; // Variable to collect user input from the prompt
	public static String path = "D:/V_Ramaraj/information_schema"; //Variable for the schema to be used at that point
	static String info = "information_schema";


	public static void main(String args[]) throws IOException
	{
		checkFileExists();
		String schemaName="not";
		do { //to get in the values every time from the user
			//check values to see if a schema has been chosen. If not chosen, send to info_schema folder. throughvar?
			System.out.print(prompt);
			userCommand = scanner.next().trim();
			String[] command= userCommand.split(" ");
			//System.out.println("Print"+command[1]);
			if(command[0].equals("show"))	//to print the schema names from the schemata table
			{
				//System.out.println("it appeared here");
				if(command[1].equals("schemas"))
				{
					showSchemas();
				}
				if(command[1].equals("tables"))
				{
					showTables(info);
				}
			}
			else if(command[0].equals("use"))	//to go to the respective schema with its tables
			{
				//System.out.println("it appeared here");
				schemaName = command[1];
				info= schemaName;
				useSchema(schemaName);
			}
			else if(command[0].equals("create"))
			{
				if(command[1].equals("schema"))	//to create a new schema
				{
					//System.out.println("it appeared here");
					createSchema(command[2]);
				}
				if(command[1].equals("table"))	//to create a new table in infoschema/new schema
				{
					if(schemaName.equals("not"))
					{
						createTable(command[2],info);	
					}
					else
					{
						createTable(command[2],schemaName);
					}
				}
			}
			else if(command[0].equals("select"))
			{
				selectValues(userCommand);
			}
			else if(command[0].equals("insert"))
			{
				
				insertValues(userCommand,command[3]);
			}
			
		} while(!userCommand.equals("exit"));
		System.out.println("Exiting...Done.");
	}

	
private static void checkFileExists(){
		// TODO Auto-generated method stub
		File fileCh = new File("D://V_Ramaraj//information_schema");
		
		String[] args=null;
		if(!fileCh.exists())
		{
			boolean retu = fileCh.mkdirs();
			InformationSchema.main(args);
		}
			
		
	}


	//takes the query, splits the values, runs a method to check the datatype, and writes the value accordingly
	//write the value into index as well how?
	private static void insertValues(String query, String tableName) throws IOException {
		// TODO Auto-generated method stub
		String[] comSplit = query.split("\\(");
		String values = comSplit[1];
		HashMap<Integer, String> columns;
		String[] name = values.split("\\)");
		values = name[0];
		String[] insertion = values.split(",");
		int length = insertion.length;
		//System.out.println(length);
		
		//System.out.println(values);
		columns = checkDatatype(tableName);
		int noOfRows = getTableTableRows(tableName);
		int noOfColumns = columns.size();
		//System.out.println("No of rows in table table: "+noOfRows);
		Set set = columns.entrySet();	//hashmap displaying values
	      Iterator iterator = set.iterator();
	      while(iterator.hasNext()) {
	         Map.Entry mentry = (Map.Entry)iterator.next();
	        // System.out.print("key is: "+ mentry.getKey() + " & Value is: ");
	         //System.out.println(mentry.getValue());
	      }
	      
//	      for(int inp=0;inp<length;inp++)
//	      {
//	    	  //don't do all of this, maybe just send the whole hashmap column????
//	    	  String data = (String) columns.get(inp+1);
	    	  putValues(columns,insertion,noOfRows-1,noOfColumns,tableName);
	    	  //get value for inp+1
	    	  //run through datatype function with the insertion[inp] value of same?
	    //  }
	}

	@SuppressWarnings("resource")
	private static int getTableTableRows(String tableName) throws IOException {
		// TODO Auto-generated method stub
		RandomAccessFile tablesTableFile = new RandomAccessFile("D:/V_Ramaraj/information_schema/information_schema.table.tbl", "rw");
		int indexTableLocation=0;
		int ij=0;
		boolean yes=false;
		char[] name=new char[30];
		byte length;
		try
		{
			for(int i=0;i<2;i++)
			{
				for(int iz=0;iz<2;iz++)
				{
					tablesTableFile.seek(indexTableLocation);
					length = tablesTableFile.readByte();
					indexTableLocation +=length+1;
					//System.out.println("You working? part 1");
				}
				if(i==0)
				{
					indexTableLocation +=8;
				}
			}
			tablesTableFile.seek(indexTableLocation);
			long schemaTableNumber = tablesTableFile.readLong();
			indexTableLocation +=8;
			tablesTableFile.seek(indexTableLocation);
			//using the rest of the rows to get in the no of rows for the given table name
			for(int i=(int)schemaTableNumber-3;i<schemaTableNumber;i++)
			{
				for(int iz=0;iz<2;iz++)
				{
					tablesTableFile.seek(indexTableLocation);
					length = tablesTableFile.readByte();
					//System.out.println("Length of table name: "+length);
					if(iz==1)
					{
						for( ij=0;ij<length;ij++)
						{

								name[ij]=(char)tablesTableFile.readByte();
						}
							String tablName = new String(name,0,ij).trim();
							//System.out.println("Table name: "+tablName);
							//System.out.println("Table Name: "+tablName);
							if(tablName.equals(tableName))
							{
								yes=true;
							}
						}
					indexTableLocation +=length+1;
					//System.out.println("You working? part 1");
				}
				if(!yes)
				{
					indexTableLocation+=8;
				}
				if(yes)
				{
					schemaTableNumber = (int)tablesTableFile.readLong();
					schemaTableNumber +=1;
					tablesTableFile.seek(indexTableLocation);
					
					/****edit this back once you have the other fucntion working ****/ // let's see if it works
					tablesTableFile.writeLong(schemaTableNumber);
				}
			}
			//System.out.println("No of rows: "+schemaTableNumber);
			tablesTableFile.close();

			return (int)schemaTableNumber;

		}
		catch(Exception e)
		{
			
		}
		return 0;
	}


	private static void putValues(HashMap<Integer, String> columns, String[] insertion, int noOfTableRows, int noOfColumns, String tableName) throws FileNotFoundException {
	// TODO Auto-generated method stub
		//since noofrows is 0, no traversing
		//first get the datatypes from the hashmap
		//then parse through those values
		//then start writing your values based on the datatype?
		
		////for now ///
		//noOfTableRows =1; 
		String path="D:/V_Ramaraj/"+info+"/"+tableName+"/"+info+"."+tableName+".tbl";
		//System.out.println(path);
		try{
			RandomAccessFile tableFile = new RandomAccessFile(path, "rw");
			byte length;
			//System.out.println("no of rows: "+noOfTableRows);
			String[] dataTypes = new String[noOfColumns];
			for(int i=0;i<noOfColumns;i++)
			{
				dataTypes[i]=columns.get(i+1);
				dataTypes[i]=dataTypes[i].trim();
				//System.out.println(dataTypes[i]);
			}
			int indexTableLocation=0;
			for(int ij=0;ij<noOfTableRows;ij++)
			{
				for(int ik=0;ik<noOfColumns;ik++)
				{
					String data = dataTypes[ik];
					//System.out.println(data);
					if(data.matches("byte"))
					{
						indexTableLocation +=1;
					}
					else if(data.matches("short"))
					{
						indexTableLocation +=2;	
					}
					else if(data.matches("long"))
					{
						indexTableLocation +=8;
					}
					else if(data.matches("date"))
					{
						tableFile.seek(indexTableLocation);
						length = tableFile.readByte();
						indexTableLocation +=length+1;	
					}
					else if(data.matches("varchar(.*)"))
					{
						int value = Integer.parseInt(data.replaceAll("[^0-9]", ""));
						//System.out.println(ik+1+": "+value);
						tableFile.seek(indexTableLocation);
						length = tableFile.readByte();
						indexTableLocation +=length+1;
					}
					else if(data.matches("int"))
					{
						indexTableLocation +=4;
					}
				}
			}
			//write  code to insert in values according to datatype
			//insertion is the string array
			
			//date did notwork, thus using string
			tableFile.seek(indexTableLocation);
			for(int ik=0;ik<noOfColumns;ik++)
			{
				//check ik values for the array since that seems to be the issue
				String data = dataTypes[ik];
				//System.out.println(data);
				if(data.matches("byte"))
				{
				//	System.out.println(insertion[ik]);
					int val = Integer.parseInt(insertion[ik]);
					tableFile.writeByte(val);
				}
				else if(data.matches("short"))
				{
				//	System.out.println(insertion[ik]);
					int val = Integer.parseInt(insertion[ik]);
					tableFile.writeShort(val);
				}
				else if(data.matches("long"))
				{
					long val= Long.parseLong(insertion[ik]);
					tableFile.writeLong(val);
				}
				else if(data.matches("date"))
				{
					tableFile.writeByte(insertion[ik].length());
					tableFile.writeBytes(insertion[ik]);			
				}
				else if(data.matches("varchar(.*)"))
				{
					//System.out.println(insertion[ik]);
//					int value = Integer.parseInt(data.replaceAll("[^0-9]", ""));
//					System.out.println(ik+1+": "+value);
//					tableFile.seek(indexTableLocation);
//					length = tableFile.readByte();
//					indexTableLocation +=length+1;
					tableFile.writeByte(insertion[ik].length());
					tableFile.writeBytes(insertion[ik]);
				}
				else if(data.matches("int"))
				{
					int val = Integer.parseInt(insertion[ik]);
					tableFile.writeInt(val);
				}
			}
			tableFile.close();
		}
		catch(Exception e)
		{
			
		}
	}


	private static HashMap<Integer, String> checkDatatype(String tableName) throws IOException {
		// TODO Auto-generated method stub
		//checks the columns table for the first time you get the table name
		//then using length traverses the column for datatypes
		//then based on the datatype, insert the length and the value accordingly
		//use same storing daatypes in array method for select and insert
		try
		{
			HashMap<Integer,String> values = new HashMap<Integer,String>();
			RandomAccessFile tablesTableFile = new RandomAccessFile("D:/V_Ramaraj/information_schema/information_schema.table.tbl", "rw");
			RandomAccessFile columnsTableFile = new RandomAccessFile("D:/V_Ramaraj/information_schema/information_schema.columns.tbl", "rw");
			char[] name = new char[30];
			int positH=0;
			int noOfColumns =0;
			int indexTableLocation=0;
			boolean yes=false;
			String colName=" ";
			byte traverseLength;
			//used to get the total number of rows in the column table
			for(int i=0;i<3;i++)
			{
				
				for(int iz=0;iz<2;iz++)
				{
					tablesTableFile.seek(indexTableLocation);
					traverseLength = tablesTableFile.readByte();
					indexTableLocation +=traverseLength+1;
					tablesTableFile.seek(indexTableLocation);	
				}
				//System.out.println((long)tablesTableFile.readLong());
				if(i==0||i==1)
				{
					indexTableLocation +=8;
				}
			}
			tablesTableFile.seek(indexTableLocation);
			long schemaTableNumber = tablesTableFile.readLong();
			//getting the value of the total numebr of rows
			//System.out.println("Did it go here?");
			int count = (int)schemaTableNumber;
			//System.out.println("no of columns rows "+count);
			indexTableLocation=0;
			//traversing the columns table, comparing the values of table_name to see if it matches the table name, then retrieving the datatype name
			for(int ix=0;ix<count;ix++)
			{
				for(int ij=0;ij<3;ij++)
				{
					columnsTableFile.seek(indexTableLocation);
					traverseLength = columnsTableFile.readByte();
					if(ij==1)
					{
						int i;
						for(i=0;i<traverseLength;i++)
						{
							name[i]=(char)columnsTableFile.readByte();
							//System.out.println("Each value: "+name[i]);
						}
						String tablName = new String(name,0,i).trim();
						//System.out.println("Table Name: "+tablName);
						if(tablName.equals(tableName))
						{
							//System.out.println("Table Name: "+tablName);
							yes = true;
						}
						else
						{
							yes=false;
						}
					}
					
					columnsTableFile.seek(indexTableLocation);
					indexTableLocation += traverseLength+1;
				}
				columnsTableFile.seek(indexTableLocation);
				int position = (int)columnsTableFile.readInt();
				if(yes)
				{
					//System.out.println("Did it go here? 2");
					positH = position;
				}
				columnsTableFile.seek(indexTableLocation);
				indexTableLocation+=4;
				 for(int ij=0;ij<3;ij++)
				 {
					 columnsTableFile.seek(indexTableLocation);
					traverseLength = columnsTableFile.readByte();
					if(ij==0 && yes)
					{
						int i;
						for(i=0;i<traverseLength;i++)
						{
							name[i]=(char)columnsTableFile.readByte();
						}
						String datatype = new String(name,0,i).trim();
						/*** change out later when done with select work***/
						//System.out.println("Datatype: "+datatype);
						values.put(positH, datatype);
					}
					indexTableLocation += traverseLength+1; 
				 }
			}
			tablesTableFile.close();
			columnsTableFile.close();
			return values;
		}
		catch(Exception e)
		{
			return null;
		}
	
	}



	private static void selectValues(String query) throws IOException {
		// TODO Auto-generated method stub
		String[] value= query.split("from|where");
		HashMap<Integer, String> columns;
		String tableName = value[1];
		for(String s: value)
		{
			s = s.replaceAll(" ","");
			//System.out.println(s);
		}
		tableName = tableName.trim();
		//System.out.println("t"+table_name+"t");
		columns = checkDatatype(tableName);
		//int noOfRows = getTableTableRows(tableName);
		int noOfColumns = columns.size();
		//System.out.println("No of rows in table table: "+noOfRows);
		Set<Entry<Integer, String>> set = columns.entrySet();	//hashmap displaying values
	      Iterator<Entry<Integer, String>> iterator = set.iterator();
	      while(iterator.hasNext()) {
	    	  //System.out.println("is it going here");
	         @SuppressWarnings("rawtypes")
			Map.Entry mentry = (Map.Entry)iterator.next();
	         //System.out.print("key is: "+ mentry.getKey() + " & Value is: ");
	         //System.out.println(mentry.getValue());
	      }
		//basically get the from part and check
		//if only from, then see aluesof datatype from columns, store somewhere, then use that to write out the data
		//if where condition exists, then see the first operator, go to that index and use treemap idk
		//this will probablydo only on tuesday
		
		//if(hashmap.containsKey())	//then only go ahead with searching for the position from the values and retrieving them
		//use an arraylist for multiple values?
	      String path;
	      if(info.contains("format"))
	      {
	    	  path="D:/V_Ramaraj/"+info+"/"+info+"."+tableName+".tbl";
	      }
	      else
	      {
	    	  path="D:/V_Ramaraj/"+info+"/"+tableName+"/"+info+"."+tableName+".tbl";  
	      }
	      
	      RandomAccessFile tableFile = new RandomAccessFile(path, "rw");
			byte length;
			//System.out.println("no of rows: "+noOfTableRows);
			String[] dataTypes = new String[noOfColumns];
			for(int i=0;i<noOfColumns;i++)
			{
				dataTypes[i]=columns.get(i+1);
				dataTypes[i]=dataTypes[i].trim();
				//System.out.println(dataTypes[i]);
			}
			int indexTableLocation=0;
			tableFile.seek(indexTableLocation);
	      while(true)
	      {
	  		
			//System.out.println(path);
			try{
				
				//for(int ij=0;ij<noOfRows;ij++)
				//{
					for(int ik=0;ik<noOfColumns;ik++)
					{
						String data = dataTypes[ik];
						//System.out.println(data);
						if(data.matches("byte"))
						{
							System.out.print((char)tableFile.readByte()+"\t");
						}
						else if(data.matches("short"))
						{
							System.out.print((short)tableFile.readShort()+"\t");
						}
						else if(data.contains("long"))
						{
							System.out.print((long)tableFile.readLong()+"\t");
						}
						else if(data.matches("date"))
						{
							//tableFile.seek(indexTableLocation);
							length = tableFile.readByte();
							for(int i=0;i<length;i++)
							{
								System.out.print((char)tableFile.readByte());
							}
							System.out.print("\t");
							//indexTableLocation +=length+1;	
						}
						else if(data.matches("varchar(.*)"))
						{
							length = tableFile.readByte();
							for(int i=0;i<length;i++)
							{
								System.out.print((char)tableFile.readByte());
							}
							System.out.print("\t");
						}
						else if(data.matches("int"))
						{
							System.out.print((int)tableFile.readInt()+"\t");
						}
					}
					System.out.println(" ");
				}
			catch(Exception e)
			{
				tableFile.close();
			}
 
	      }
			
	}



	private static void showTables(String info) throws IOException {
		// TODO Auto-generated method stub
		String schemaN = info;
		int indexLocation = 0, length,ix;
		char[] nameCheck=new char[40];
		RandomAccessFile tablesTableFile = new RandomAccessFile("D:/V_Ramaraj/information_schema/information_schema.table.tbl", "rw");
		try
		{
			//we read in the name of the schema first and compared it with the schema which is open
			//we then get the length of the table name. if the schema matches, then it is printed, otherwise it's skipped till EOF
			while(true)
			{
				tablesTableFile.seek(indexLocation);
				//put this into the loop, write the length part in the end after the if condition?
				length = tablesTableFile.readByte();
				//System.out.println(length);
				for(ix=0;ix<length;ix++)
				{
					nameCheck[ix]=(char)tablesTableFile.readByte();
					//i++;
				}
				String fileSchemaName = new String(nameCheck,0,ix).trim();
				indexLocation += length+1;
				tablesTableFile.seek(indexLocation);
				length = tablesTableFile.readByte();
				//System.out.println(length);
				//read the next byte plus bytes into array. if schema equals then print it
				if(schemaN.equals(fileSchemaName))
				{
					//System.out.println("It works!");
					for(ix=0;ix<length;ix++)
					{
						System.out.print((char)tablesTableFile.readByte());
						//i++;
					}
					System.out.print("\n");
				}
				indexLocation+=length+1+8;
		}

		}
		catch(Exception e)
		{
			
			//System.out.println(e);
			//System.out.println("Done");
			tablesTableFile.close();
		}
	}

	private static void createTable(String tableName, String schemaName) throws IOException 
	{
		// TODO Auto-generated method stub
		String command = userCommand;
		System.out.println("Schema Name "+schemaName);
		System.out.println("Path "+path); 
		new File(path+"\\"+tableName).mkdir();
		String[] comSplit= command.split(" ");
		//System.out.println("Length: "+comSplit.length);
		//not null is adding one more  column
		int i=0;
		int col=0,data=0;
		int countEach=0,countAll=0;
		int noOfColumns=0;
		HashMap<Integer, Integer> initialValues = new HashMap<Integer, Integer>();
		for(int s=3;s<comSplit.length;s++)	//used to parse through each string word and count the number
		{
				comSplit[s].replaceAll(" ", "");
				if(!comSplit[s].matches("(.*),"))
				{
					countEach++;
					countAll++;
				//	System.out.println("Value: "+s);
				}
				else
				{
					countEach++;
					countAll++;
					//System.out.println(s);
					//check = i-check -2-countEach+1; //check this logic out
					noOfColumns++;
					//System.out.println("See if this matches: "+check);
					//System.out.println("Each row: "+countEach);
					initialValues.put(noOfColumns, countEach);
					countEach =0;
				}

				if(s==comSplit.length-1)
				{

					noOfColumns++;
					//System.out.println("See if this matches: "+check);
					//System.out.println("Each row: "+countEach);
					initialValues.put(noOfColumns, countEach);
					countEach =0;	
				}
				//System.out.println(count);	
			}
		
		//System.out.println("No of total values: "+countAll);

		Set set = initialValues.entrySet();	//hashmap displaying values
	      Iterator iterator = set.iterator();
	      while(iterator.hasNext()) {
	         Map.Entry mentry = (Map.Entry)iterator.next();
	         //System.out.print("key is: "+ mentry.getKey() + " & Value is: ");
	         //System.out.println(mentry.getValue());
	      }
		
	      String[][] insert = new String[noOfColumns][4];
	      //System.out.println("No of rows: "+noOfColumns);
	      
	      int stringCount=3;
	      comSplit[3]=comSplit[3].replaceAll("\\(", "");
	      int  removeBracket = countAll +2;
	      comSplit[removeBracket]=comSplit[removeBracket].replaceAll("\\)", "");
	      
	      //uses the hashmap value to get the name, datatype, nullable and primary key value into a multidimensional array
	      for(int ix=0;ix<noOfColumns;ix++)
	      {
	    	  int getV = initialValues.get(ix+1);
	    	  //if there are 4 values in the column thing, it means there's a primary key or a not null value
	    	  if(getV==4)
	    	  {

	    			  insert[ix][0]= comSplit[stringCount].replaceAll(" ", "");
	    			  stringCount++;	//is 4 here
	    			  insert[ix][1]= comSplit[stringCount].replaceAll(" ", "");
	    			  stringCount++; //is 5 here
	    			  if(comSplit[stringCount].matches("(.*)rima(.*)"))
	    			  {
	    				  insert[ix][2]= "No";
	    				  insert[ix][3]="Pri";
	    			  }
	    			  else if(comSplit[stringCount].matches("(.*)ot"))
	    			  {
	    				  insert[ix][2]= "No";
	    				  insert[ix][3]="";
	    			  }
	    			  stringCount=stringCount+2;	//1st of next line
	    		  }
	    	  //if there are only 2 values, it means it is nullable and there is no primary key
	    	  	if(getV==2)
	    	  	{
	    			  insert[ix][0]= comSplit[stringCount].replaceAll(" ", "");
	    			  stringCount++;	//is 4 here
	    			  insert[ix][1]= comSplit[stringCount].replaceAll(" ", "");
	    			  insert[ix][1]= insert[ix][1].replaceAll(",", "");
	    			  insert[ix][2] = "Yes";
	    			  insert[ix][3] = "";
	    			  stringCount++;

	    	  	}
	      }
	      
//since we have already created the files, don't do it again while testing
	      
	      String name = schemaName+"."+tableName+".tbl";
	     File file =  new File(path+"/"+tableName+"/"+name);
	     file.createNewFile();
	      for(int ia=0;ia<noOfColumns;ia++)
	      {
	    	  name = schemaName+"."+tableName+"."+insert[ia][0]+".ndx";
	    	  //System.out.println(name);
	    	 File file1 =  new File(path+"/"+tableName+"/"+name);
	    	 file1.createNewFile();
	      }
	      
	     addRowToTablesTables(tableName,schemaName);	//while editing the columns table and such
	      addRowtoColumnsTables(tableName,schemaName,insert,noOfColumns);
		//find primary key pattern in all the rest
		//keep adding values to see how many columns exist
		//write functions to add the values to columns table, tables table 
	} //end of create table class



	private static void addRowtoColumnsTables(String tableName, String schemaName, String[][] insert, int noOfColumns) throws IOException {
		// TODO Auto-generated method stub
		try {
			RandomAccessFile tablesTableFile = new RandomAccessFile("D:/V_Ramaraj/information_schema/information_schema.table.tbl", "rw");
			RandomAccessFile columnsTableFile = new RandomAccessFile("D:/V_Ramaraj/information_schema/information_schema.columns.tbl", "rw");
			int indexTableLocation=0;
			byte length;
			//get no of rows from the tables table
			for(int i=0;i<3;i++)
			{
				for(int iz=0;iz<2;iz++)
				{
					tablesTableFile.seek(indexTableLocation);
					length = tablesTableFile.readByte();
					indexTableLocation +=length+1;
					tablesTableFile.seek(indexTableLocation);	
				}
				//System.out.println((long)tablesTableFile.readLong());
				if(i==0||i==1)
				{
					indexTableLocation +=8;
				}
			}
			tablesTableFile.seek(indexTableLocation);
			long schemaTableNumber = tablesTableFile.readLong();
			long noOfRows=schemaTableNumber+noOfColumns;
			tablesTableFile.seek(indexTableLocation);
			tablesTableFile.writeLong(noOfRows);
			//System.out.println((long)tablesTableFile.readLong());
			int count = (int)schemaTableNumber;
			indexTableLocation=0;
			for(int ix=0;ix<count;ix++)
			{
				for(int ij=0;ij<3;ij++)
				{
					columnsTableFile.seek(indexTableLocation);
					length = columnsTableFile.readByte();
					indexTableLocation += length+1;
				}
				columnsTableFile.seek(indexTableLocation);
				//System.out.println("Ordinal Position: "+(int)columnsTableFile.readInt());
				indexTableLocation+=4;
				 for(int ij=0;ij<3;ij++)
				 {
					 columnsTableFile.seek(indexTableLocation);
					length = columnsTableFile.readByte();
					indexTableLocation += length+1; 
				 }
			}
			columnsTableFile.seek(indexTableLocation);
			for(int iy=0;iy<noOfColumns;iy++)
			{
				columnsTableFile.writeByte(schemaName.length());
				columnsTableFile.writeBytes(schemaName);
				columnsTableFile.writeByte(tableName.length());
				columnsTableFile.writeBytes(tableName);
				columnsTableFile.writeByte(insert[iy][0].length());
				columnsTableFile.writeBytes(insert[iy][0]);
				columnsTableFile.writeInt(iy+1);
				columnsTableFile.writeByte(insert[iy][1].length());
				columnsTableFile.writeBytes(insert[iy][1]);
				columnsTableFile.writeByte(insert[iy][2].length());
				columnsTableFile.writeBytes(insert[iy][2]);
				columnsTableFile.writeByte(insert[iy][3].length());
				columnsTableFile.writeBytes(insert[iy][3]);
			}
	
		} 
		catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}



	private static void addRowToTablesTables(String tableName, String schemaName) throws IOException {
		// TODO Auto-generated method stub
		RandomAccessFile tablesTableFile = new RandomAccessFile("D:/V_Ramaraj/information_schema/information_schema.table.tbl", "rw");
		int indexTableLocation=0;
		byte length;
		try
		{
			for(int i=0;i<2;i++)
			{
				for(int iz=0;iz<2;iz++)
				{
					tablesTableFile.seek(indexTableLocation);
					length = tablesTableFile.readByte();
					indexTableLocation +=length+1;
					//System.out.println("You working? part 1");
				}
				if(i==0)
				{
					indexTableLocation +=8;
				}
			}
			tablesTableFile.seek(indexTableLocation);
			long schemaTableNumber = tablesTableFile.readLong();
			//System.out.println("No of rows: "+schemaTableNumber);
			long add = schemaTableNumber+1;
			tablesTableFile.seek(indexTableLocation);
			tablesTableFile.writeLong(add);
			indexTableLocation +=8;
			for(int ij=0;ij<schemaTableNumber-2;ij++)
			{
				for(int iz=0;iz<2;iz++)
				{
					tablesTableFile.seek(indexTableLocation);
					length = tablesTableFile.readByte();
					//System.out.println(length);
					indexTableLocation +=length+1;
					//System.out.println("You working? part 1");
				}
				tablesTableFile.seek(indexTableLocation);
				
					indexTableLocation += 8;
			}
			//write code here for the length, schema name,length,table name, and no of rows = 0 as long
			tablesTableFile.seek(indexTableLocation);
			tablesTableFile.writeByte(schemaName.length());
			tablesTableFile.writeBytes(schemaName);
			tablesTableFile.writeByte(tableName.length());
			tablesTableFile.writeBytes(tableName);
			long noOfRows=0;
			tablesTableFile.writeLong(noOfRows);	
		}
		catch(Exception e)
		{
			tablesTableFile.close();
		}
	}



	private static void createSchema(String schemaName) //this creates a folder in the davisql folder with the name
	{
		int indexTableLocation = 0,indexSchemataLocation=0;
		// TODO Auto-generated method stub
		new File("D:\\V_Ramaraj\\"+schemaName).mkdir();	//create a directory for the tables in that schma essentially

		try
		{
			byte length;
			/*here, we read in hard coded 2 values till the end of the first row for the table 
			to update the number of rows in the tables table in the information schema */
			RandomAccessFile schemataTableFile = new RandomAccessFile("D:/V_Ramaraj/information_schema/information_schema.schemata.tbl", "rw");
			RandomAccessFile tablesTableFile = new RandomAccessFile("D:/V_Ramaraj/information_schema/information_schema.table.tbl", "rw");
			for(int iz=0;iz<2;iz++)
			{
				tablesTableFile.seek(indexTableLocation);
				length = tablesTableFile.readByte();
				indexTableLocation +=length+1;
			}
			tablesTableFile.seek(indexTableLocation);
			long schemaTableNumber = tablesTableFile.readLong();
			tablesTableFile.seek(indexTableLocation);
			schemaTableNumber = schemaTableNumber+1;
			tablesTableFile.writeLong(schemaTableNumber);
//			tablesTableFile.seek(indexTableLocation);
//			schemaTableNumber = tablesTableFile.readLong();
//			System.out.println("Number of rows: "+schemaTableNumber);
			tablesTableFile.close();
			
			//once we have written in the value, we then can use the number of rows to loop through the schemata table and then write values there
			byte schemaLength;
			for(int iy=0;iy<schemaTableNumber-1;iy++)
			{
				schemataTableFile.seek(indexSchemataLocation);
				schemaLength = schemataTableFile.readByte();
				indexSchemataLocation +=schemaLength+1;
			}
			schemataTableFile.seek(indexSchemataLocation);
			schemataTableFile.writeByte(schemaName.length());
			schemataTableFile.writeBytes(schemaName);
			schemataTableFile.close();
		}
		catch(Exception e)
		{
			System.out.println(e);
		}
		
	}

	private static void useSchema(String schemaName) //this takes the schema name so that it can go into that folder
	{
		// TODO Auto-generated method stub
		path = "D:/V_Ramaraj/"+schemaName;
		System.out.println("You can now modify the tables from the "+schemaName+" schema");	

		//write code here to basically see the table folders in that table
		
	}

	private static void showSchemas() throws IOException //used to display the schemas from the infoschemas schemata table
	{
		//System.out.println("It works");
		// TODO Auto-generated method stub
		int schemaLocation =0;
		RandomAccessFile schemataTableFile = new RandomAccessFile("D:/V_Ramaraj/information_schema/information_schema.schemata.tbl", "rw");
		try
		{
			//write code for indexlocation, seek the last one and continue writing until true, on catch, write some  command for development
			
			while(true)
			{
				schemataTableFile.seek(schemaLocation);
				byte schemaNameLength = schemataTableFile.readByte();
				for(int ix =0;ix<schemaNameLength;ix++)
				{
					System.out.print((char)schemataTableFile.readByte());
				}
				System.out.print("\n");
				schemaLocation +=schemaNameLength+1;
			}
				
		}
		catch(Exception e)
		{
			schemataTableFile.close();
			//System.out.println(e);
		}
		
	}
}
