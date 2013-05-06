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

package gov.ornl.keva.examples.index;

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
import gov.ornl.keva.node.KevaDBFactory;
import gov.ornl.keva.node.KevaDB;

/**
 * Count all the words from a set of text files. This application shows
 * you how to use multiple threads to store a list of values. 
 */
public class ReverseIndex {

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
     * Path of text files. 
     */
    private String dir;

    /**
     * @param config Keva database configuration file path
     */
    public ReverseIndex(String config, String dir) {
	dbConfig = config;
	this.dir = dir;

	openOptions = new OpenOptions();
	writeOptions = new WriteOptions();

	openOptions.deleteIfExists = true;
	openOptions.errorIfExists = false;
    }

    /**
     * Transform integer into a value. 
     */
    private TableValue createValue(int count) {
	TableValue value = TableValueFactory.fromInt(count);
	value.setClock(new VectorClock("me".getBytes(), 0));

	return value;	
    }

    /**
     * Transform value back into integer. 
     */
    private int fromValue(TableValue value) {
	ByteBuffer buffer = ByteBuffer.wrap(value.getData());
	return buffer.getInt();
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

    /**
     * Start counting the documents. 
     */
    public void run() {
	// First open the database.
	open("index");

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

	// Print out the results.
	iterateAndPrint();

	// Close the database. 
	try {
	    keva.close();
	} catch(KevaDBException e) {
	    e.printStackTrace();
	}
    }

    /**
     * Scan an individual file. 
     */
    private void scanFile(File file) {
	BufferedReader reader = null;
	try {
	    reader = 
		new BufferedReader(new FileReader(file));
	} catch(FileNotFoundException e) {
	    e.printStackTrace();
	    return;
	}
       
	String line = null;
	int pos =0;
	long start = System.currentTimeMillis();

	do {
	    try {
		line = reader.readLine();
	    }  catch(IOException e) {
		e.printStackTrace();
	    }

	    if(line != null) {
		// Split the line into words.
		String[] words = line.split(" ");

		// Now for each word, emit a "1" value. 
		for(String word : words) {
		    if(!word.trim().equals("")) {
			// Remove any punctuation. 
			word = word.replaceAll("[^A-Za-z]", "");
			
			// Create the relevant keys. 
			TableKey tk = TableKeyFactory.fromString(word);
			TableValue tv = createValue(pos++);
			writeOptions.branch = file.getName();
			keva.put(tk, tv, writeOptions);
		    }
		}
	    }

	} while(line != null);
	long end = System.currentTimeMillis();
	System.out.printf("input took %.3f sec.\n", (double)(end - start) / 1000.00);
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
     * Iterate through each key and print out the counts. 
     */
    private void iterateAndPrint() {
	Iterator<TableKey> iter = keva.iterator();
	while(iter.hasNext()) {
	    TableKey tk = iter.next();

	    // Read in the values. 
	    Map<String, StreamIterator<TableValue>> values = 
		keva.getHistory(tk);

	    System.out.printf("%s\n", tk.toString());
	    for(String doc : values.keySet()) {

		StreamIterator<TableValue> valueIter = values.get(doc);
		System.out.printf("   -- %s (", doc);
		if(valueIter.size() <= 5) {
		    while(valueIter.hasNext()) {
			int pos = fromValue(valueIter.next());
			System.out.printf("%d ", pos);
		    }
		}
		else {
		    System.out.printf(" > 5 ");
		}

		System.out.printf(")\n");
	    }
	}
    }

    /**
     * Run the word count. Assume that the first two arguments
     * provide database config and input file(s)
     */
    public static void main(String[] args) {
	if(args.length == 2) {
	    ReverseIndex wc = new ReverseIndex(args[0].trim(), args[1].trim());
	    wc.run();
	}
	else {
	    System.out.printf("num args %d\n", args.length);
	}
    }
}