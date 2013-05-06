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

package gov.ornl.keva.examples.sensor;

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
 * Demonstrate how to use the write batch by simulating sensor data collection.
 * The application needs to insert multiple items (data + metadata) in a consistent
 * manner across keys and branches. 
 *
 * @author James Horey
 */
public class SensorCollect {

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
     * Fake sensor data. 
     */
    private byte[][] data;
    private int[] sizes;
    private byte[][] sensors;
    private String[] experiments;

    /**
     * Fixed fake sensor limits.
     */
    private static final int MAX_EXPERIMENTS = 10;
    private static final int MAX_DATA_POINTS = 100;
    private static final int MAX_SAMPLES = 75;
    private static final int NUM_WORKERS = 20;

    private static final Random rand = 
	new Random(System.currentTimeMillis());

    /**
     * @param config Keva database configuration file path
     */
    public SensorCollect(String config) {
	dbConfig = config;

	// Create some fake sensor data.
	createSensorData();

	openOptions = new OpenOptions();
	writeOptions = new WriteOptions();

	openOptions.deleteIfExists = true;
	openOptions.errorIfExists = false;
    }

    /**
     * Create some fake sensor data. 
     */
    private void createSensorData() {
	experiments = new String[MAX_EXPERIMENTS];
	data = new byte[MAX_DATA_POINTS][];
	sensors = new byte[MAX_DATA_POINTS][];
	sizes = new int[MAX_DATA_POINTS];

	for(int i = 0; i < MAX_DATA_POINTS; ++i) {
	    byte[] d = createData();
	    byte[] s = createSensor();

	    data[i] = d;
	    sensors[i] = s;
	    sizes[i] = rand.nextInt(500);
	}

	for(int i = 0; i < MAX_EXPERIMENTS; ++i) {
	    experiments[i] = String.format("experiment-%d", i);
	}
    }

    /**
     * Create some fake sensor data.
     */
    private byte[] createData() {
	ByteBuffer buffer = ByteBuffer.allocate(Long.SIZE / 8);
	buffer.putLong(rand.nextLong());
	
	return buffer.array();
    }

    /**
     * Create some fake sensor IDs.
     */
    private byte[] createSensor() {
	String s = String.format("sensor-%d", rand.nextInt(100));
	return s.getBytes();
    }

    /**
     * Transform integer into a value. 
     */
    private TableValue createValue(int count, int sample) {
	String id = String.format("%d", Thread.currentThread().getId());
	ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE / 8);
	buffer.putInt(count);

	TableEmbedded value = new TableEmbedded();
	value.setData(buffer.array());
	value.setClock(new VectorClock(id.getBytes(), sample));

	return value;	
    }

    /**
     * Transform byte[] into a value. 
     */
    private TableValue createValue(byte[] data, int sample) {
	String id = String.format("%d", Thread.currentThread().getId());
	TableEmbedded value = new TableEmbedded();
	value.setData(data);
	value.setClock(new VectorClock(id.getBytes(), sample));

	return value;	
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
     * Transform value back into integer. 
     */
    private int intFromValue(TableValue value) {
	ByteBuffer buffer = ByteBuffer.wrap(value.getData());
	return buffer.getInt();
    }

    /**
     * Transform value back into byte array. 
     */
    private byte[] byteFromValue(TableValue value) {
	return value.getData();
    }

    /**
     * Create a new write batch representing some sensor values.
     */
    private synchronized WriteBatch readSensorData(int s) {
	WriteBatch batch = new WriteBatch();

	// int dataPoint = rand.nextInt(MAX_DATA_POINTS);
	int dataPoint = (int)Thread.currentThread().getId() % Integer.MAX_VALUE;
	String experiment = experiments[rand.nextInt(MAX_EXPERIMENTS)];

	// Add the actual sensor data.
	TableKey key = TableKeyFactory.fromString(experiment);
	batch.addWrite(key, 
		       createValue(data[dataPoint], s),
		       createOptions("data"));

	// Add the name of the sensor.
	batch.addWrite(key, 
		       createValue(sensors[dataPoint], s),
		       createOptions("sensor"));
	

	// Add the "size" information.
	batch.addWrite(key, 
		       createValue(sizes[dataPoint], s),
		       createOptions("size"));

	// Add the index information.	

	batch.addWrite(key, 
		       createValue(dataPoint, s),
		       createOptions("index"));

	
	return batch;
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
	open("sensors");
	
	// Sample from the sensor data.
	try {
	    List<Thread> workers = new ArrayList<>(NUM_WORKERS);
	    for(int i = 0; i < NUM_WORKERS; ++i) {
		Thread worker = new Thread(new SensorGenerator());
	    
		worker.start();
		workers.add(worker);	    
	    }

	    // Wait for them to finish. 
	    for(Thread worker : workers) {
		worker.join();
	    }
	}  catch(InterruptedException e) {
	    e.printStackTrace();
	}

	// Print out the results.
	iterateAndPrint();

	// Close the database. 
	try {
	    System.out.printf("closing database\n");
	    keva.close();
	} catch(KevaDBException e) {
	    e.printStackTrace();
	}
    }

    /**
     * Iterate through each key and print out the counts. 
     */
    private void iterateAndPrint() {
	Iterator<TableKey> iter = keva.iterator();
	while(iter.hasNext()) {
	    TableKey tk = iter.next();

	    // Get list of the experiments. Branches are the
	    // different types (size, index, etc.). 
	    Map<String, StreamIterator<TableValue>> values = 
		keva.getHistory(tk);

	    if(values.size() == 0) {
		return;
	    }

	    System.out.printf("%s --> ", tk.toString());

	    StreamIterator<TableValue> dataIter = values.get("data");
	    StreamIterator<TableValue> sensorIter = values.get("sensor");
	    StreamIterator<TableValue> sizeIter = values.get("size");
	    StreamIterator<TableValue> indexIter = values.get("index");

	    boolean aOk = true;	    
	    while(dataIter.hasNext() &&
		  sensorIter.hasNext() &&
		  sizeIter.hasNext() &&
		  indexIter.hasNext()) {
		
		TableValue indexValue = indexIter.next();
		TableValue sizeValue = sizeIter.next();
		TableValue dataValue = dataIter.next();
		TableValue sensorValue = sensorIter.next();

		int i = intFromValue(indexValue);
		int size = intFromValue(sizeValue);
		byte[] results = byteFromValue(dataValue);
		byte[] sensor = byteFromValue(sensorValue);
		
		// int i = intFromValue(indexIter.next());
		// int size = intFromValue(sizeIter.next());
		// byte[] results = byteFromValue(dataIter.next());
		// byte[] sensor = byteFromValue(sensorIter.next());

		if(!Arrays.equals(results, data[i]) ||
		   !Arrays.equals(sensor, sensors[i]) ||
		   size != sizes[i]) {
		    aOk = false;

		    // This is not good. Somewhere along the line the batch
		    // got mixed up with other writes :(
		    System.out.printf("fail\n");
		    System.out.printf("---------------------------------------------------------\n");
		    System.out.printf("( %s  %s %s )\n", 
				      sizeValue.getClock().toString(), 
				      dataValue.getClock().toString(), 
				      sensorValue.getClock().toString());

		    System.out.printf("( %-3s  %-45s %-20s )\n", 
				      size, 
				      Arrays.toString(results), 
				      new String(sensor));

		    System.out.printf("( %-3s  %-45s %-20s )\n", 
				      sizes[i], 
				      Arrays.toString(data[i]), 
				      new String(sensors[i]));
		    System.out.printf("---------------------------------------------------------\n");
		}
	    }

	    if(aOk) {
		System.out.printf("success\n");
	    }
	}
    }

    /**
     * Generate the sensor data. 
     */
    class SensorGenerator implements Runnable {
	/**
	 * Start counting the documents. 
	 */
	public void run() {
	    // Sample from the sensor data.
	    for(int i = 0; i < MAX_SAMPLES; ++i) {
		if(!keva.put(readSensorData(i))) {
		    // The batch commit failed. Try again
		    // later or just quit. For the purposes of
		    // this small demo, just quit. 
		}
	    }
	}
    }

    /**
     * Run the word count. Assume that the first two arguments
     * provide database config and input file(s)
     */
    public static void main(String[] args) {
	if(args.length == 1) {
	    SensorCollect wc = new SensorCollect(args[0].trim());
	    wc.run();
	}
	else {
	    System.out.printf("num args %d\n", args.length);
	}
    }
}