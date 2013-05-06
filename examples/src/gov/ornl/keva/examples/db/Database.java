/**
 * Copyright 2013 Oak Ridge National Laboratory
 * Author: James Horey <horeyjl@ornl.gov>
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
**/

package gov.ornl.keva.examples.database;

/**
 * Java libs.
 **/
import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import java.util.Arrays;

/**
 * Keva libs.
 **/
import gov.ornl.keva.table.TableKey;
import gov.ornl.keva.table.TableValue;
import gov.ornl.keva.table.TableKeyFactory;
import gov.ornl.keva.table.TableValueFactory;
import gov.ornl.keva.table.TableEmbedded;
import gov.ornl.keva.core.KevaDBException;
import gov.ornl.keva.core.VectorClock;
import gov.ornl.keva.core.WriteOptions;
import gov.ornl.keva.core.OpenOptions;
import gov.ornl.keva.core.StreamIterator;
import gov.ornl.keva.node.WriteBatch;
import gov.ornl.keva.node.KevaDBFactory;
import gov.ornl.keva.node.KevaDB;

/**
 * Import CSV-style files and organize the data by rows. This is not
 * really a "database", but gives an impression of how one might store such
 * data in Keva.
 *
 * @author James Horey
 */
public class Database {

    /**
     * Benchmark controls.
     */
    private OpenOptions openOptions;
    private WriteOptions writeOptions;

    /**
     * Keva controls. 
     */
    private KevaDBFactory factory;
    private KevaDB keva;
    private String dbConfig;

    /**
     * File/dir containing the database files. 
     */
    private String dir;

    /**
     * @param config Keva database configuration file path
     */
    public Database(String config, String path) {
	dbConfig = config;
	dir = path;

	openOptions = new OpenOptions();
	writeOptions = new WriteOptions();
    }

    /**
     * Transform String[] into a value. 
     */
    private TableValue createValue(String[] words) {
	int length = 0;
	byte[][] buffers = new byte[words.length][];
	for(int i = 0; i < words.length; ++i) {
	    buffers[i] = words[i].getBytes();
	    length += buffers[i].length;
	}

	ByteBuffer buffer = ByteBuffer.allocate( (words.length * (Integer.SIZE/ 8)) + 
						 length);
	for(byte[] b : buffers) {
	    buffer.putInt(b.length);
	    buffer.put(b);
	}
	TableEmbedded value = new TableEmbedded();
	value.setData(buffer.array());
	value.setClock(new VectorClock("me".getBytes(), 0));

	return value;	
    }

    /**
     * Transform byte[] into a value. 
     */
    private TableValue createValue(byte[] data) {
	TableEmbedded value = new TableEmbedded();
	value.setData(data);
	value.setClock(new VectorClock("me".getBytes(), 0));

	return value;	
    }

    /**
     * Translate back into database row. 
     */
    private DatabaseRow fromValue(TableValue v) {
	DatabaseRow dbRow = new DatabaseRow();
	byte[] data = v.getData();
	ByteBuffer buffer = ByteBuffer.wrap(data);

	do {
	    // Retrieve the word buffer. 
	    int length = buffer.getInt();
	    byte[] wordBuffer = new byte[length];
	    buffer.get(wordBuffer);

	    // Now insert into the database row.
	    dbRow.addData(new String(wordBuffer));
	} while(buffer.remaining() > 0);

	return dbRow;
    }

    /**
     * Create some new write options.
     */
    public WriteOptions createOptions(String branch) {
	WriteOptions options = new WriteOptions();
	options.branch = branch;

	return options;
    }

    /**
     * Transform value back into byte array. 
     */
    private byte[] byteFromValue(TableValue value) {
	return value.getData();
    }

    /**
     * Open the database. 
     */
    private void open(String db) {
	try {
	    factory = new KevaDBFactory(dbConfig);
	    keva = factory.open(db, openOptions);
	} catch(KevaDBException e) {
	    e.printStackTrace();
	}
    }

    public void run(String cmd) {
	if(cmd.equals("import")) {
	    importDatabase();
	}
	else if(cmd.equals("confirm")) {
	    confirmDatabase();
	}
	else if(cmd.equals("export")) {
	    exportDatabase();
	}
    }

    /**
     * Run some sample queries. 
     */
    public void confirmDatabase() {
	Path path = Paths.get(dir);
	File file = new File(path.toAbsolutePath().toString());
	BufferedReader reader = null;

	try {
	    reader = 
		new BufferedReader(new FileReader(file));
	} catch(FileNotFoundException e) {
	    e.printStackTrace();
	    return;
	}

	int rowNum = 0;
	String line = null;
	Map<Integer, String> input = new HashMap<>();
	do {       
	    try {
		line = reader.readLine();

		if(rowNum % 20000 == 0) {
		    input.put(rowNum, line);
		}
	    }  catch(IOException e) {
		e.printStackTrace();
	    }
	    rowNum++;
	} while(line != null);

	openOptions.deleteIfExists = false;
	openOptions.errorIfExists = false;
	open(file.getName());

	int numKeys = 0;
	Iterator<TableKey> iter = keva.iterator();
	while(iter.hasNext()) {
	    TableKey tk = iter.next();
	    numKeys++;
	}
	System.out.printf("numKeys:%d\n", numKeys);

	for(int i = 0; i < 10; ++i) {
	    rowNum = i * 20000;
	    TableKey tk = TableKeyFactory.fromInt(rowNum);

	    Iterator<TableValue> valueIter = 
		keva.get(tk, "m0");	    

	    while(valueIter.hasNext()) {
		TableValue v = valueIter.next();
		DatabaseRow row = fromValue(v);

		System.out.printf("row:%d -------------------------------------------\n", i * 20000);
		System.out.printf(row.toString() + "\n");
		System.out.printf(input.get(rowNum) + "\n");
		System.out.printf("-------------------------------------------\n");
	    }

	}

	try {
	    System.out.printf("closing database\n");
	    keva.close();
	} catch(KevaDBException e) {
	    e.printStackTrace();
	}
    }

    /**
     * Export databases back to a CSV file. 
     */
    public void exportDatabase() {
	openOptions.deleteIfExists = false;
	openOptions.errorIfExists = false;
	open(dir);

	Iterator<TableKey> iter = keva.iterator();
	while(iter.hasNext()) {
	    TableKey tk = iter.next();

	    // Get list of the experiments. Branches are the
	    // different types (size, index, etc.). 
	    // Map<String, StreamIterator<TableValue>> values = 
	    // 	keva.get(tk);
	    Iterator<TableValue> valueIter = 
		keva.get(tk, "m0");

	    while(valueIter.hasNext()) {
		TableValue v = valueIter.next();
		DatabaseRow row = fromValue(v);
		System.out.printf(row.toString());
	    }

	    System.out.printf("\n");
	}

	// Close the database. 
	try {
	    keva.close();
	} catch(KevaDBException e) {
	    e.printStackTrace();
	}
    }

    /**
     * Start counting the documents. 
     */
    public void importDatabase() {	
	// Did the user supply a directory or file? 
	Path path = Paths.get(dir);
	if(path == null) {
	    System.out.printf("could not find input directory %s\n", dir);
	}

	File file = new File(path.toAbsolutePath().toString());
	if(file.isDirectory()) {
	    scanDirectory(file);
	}
	else {
	    scanFile(file);
	}

	// Close the database. 
	try {
	    System.out.printf("closing database\n");
	    keva.close();
	} catch(KevaDBException e) {
	    e.printStackTrace();
	}
    }

    /**
     * Scan an individual file. 
     */
    private void scanFile(File file) {
	// First open the database.
	openOptions.deleteIfExists = true;
	openOptions.errorIfExists = false;
	open(file.getName());

	BufferedReader reader = null;
	try {
	    reader = 
		new BufferedReader(new FileReader(file));
	} catch(FileNotFoundException e) {
	    e.printStackTrace();
	    return;
	}
       
	int row = 0;
	String line = null;
	long start = System.currentTimeMillis();

	do {
	    try {
		line = reader.readLine();
	    }  catch(IOException e) {
		e.printStackTrace();
	    }

	    if(line != null) {
		// Split the line into words.
		String[] words = line.split("[|]");

		// Now for each word, emit a "1" value.
		TableKey tk = TableKeyFactory.fromInt(row);

		TableValue tv = createValue(words);
		writeOptions.branch = "m0";
		keva.put(tk, tv, writeOptions);
	    }

	    // Increment the row ID. 
	    row++;

	    if(row % 1000 == 0) {
	    	System.out.printf("%d..", row);
	    }

	} while(line != null);
	System.out.printf("\n");

	long end = System.currentTimeMillis();
	System.out.printf("read took %.3f sec.\n", (double)(end - start) / 1000.00);
    }

    /**
     * Scan the directory for text files and scan
     * each text file. 
     */
    private void scanDirectory(File dir) {
	String[] entries = dir.list();
	
	for(String entry : entries) {
	    File file = new File(dir + "/" + entry);
	    if(file.isFile()) {
		System.out.printf("scanning file %s\n", file.getName());
		scanFile(file);
	    }
	}
    }

    /**
     * Simple row implementation. Only supports strings and numbers. 
     */
    class DatabaseRow {
	private List<Object> data;

	public DatabaseRow() {
	    data = new ArrayList<>();
	}

	public void addData(Object d) {
	    data.add(d);
	}

	public int size() {
	    return data.size();
	}

	public String toString() {
	    String s = "";

	    for(Object d : data) {
		s += d.toString() + "|";
	    }

	    return s;
	}
    }

    /**
     * Run the word count. Assume that the first two arguments
     * provide database config and input file(s)
     */
    public static void main(String[] args) {
	Database wc = new Database(args[0].trim(),
				   args[1].trim());

	wc.run(args[2].trim());
    }
}