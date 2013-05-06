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

package gov.ornl.keva.table;

/**
 * Java libs. 
 **/
import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Iterator;

/**
 * Keva libs.
 **/
import gov.ornl.keva.core.VectorClock;
import gov.ornl.keva.core.StreamIterator;
import gov.ornl.keva.core.PruneOptions;

/**
 * A non-thread-safe bucket implementation used by the sstable
 * readers. When reading from an sstable we know that there is
 * only a single writer. 
 *
 * @author James Horey
 */
public class UnsafeBucket extends TableBucket {
    private volatile int num = 0;

    /**
     * Store the actual value histories, and also a separate branch name
     * index. Both of these datastructures must be thread safe since we may
     * perform concurrent iterations while items are added.
     **/
    private final NavigableMap<String, TableValueHistory> entries = 
	new TreeMap<>();

    /**
     * @param comp Compare independent table values
     */
    public UnsafeBucket() {
	super();
    }

    /**
     * Commit the value identified by vector clock and branch. 
     * Not implemented.
     * 
     * @param value Table value to commit
     * @param branch Specific branch where the value resides (optional)
     */
    @Override public boolean commit(final TableValue value, 
				    final String branch) {
	return false;
    }

    /**
     * Add a new value to this bucket to a specific branch.
     * This means that the vector clock associated with the
     * value should be appended to the branch vector clock. 
     * 
     * @param value Table value to add to bucket
     * @param branch Specific branch where the value resides (optional)
     */
    @Override public void add(final TableValue value,
			      final String branch) {
	// Find the latest vector clock associated with the branch,
	// and then the latest value history. 
	TableValueHistory history = null;

	history = entries.get(branch);
	if(history == null) {
	    if(value.getFlags() != TableValue.TENTATIVE) {
		// This must be a new branch. 
		history = new UnsafeTableValueHistory();
		history.setBranchName(branch);
		entries.put(branch, history);
	    }
	}
	
	// Add the value to our history. 
	history.add(value);

	// Increase our committed count
	if(value.getFlags() != TableValue.TENTATIVE) {
	    num++;
	}
    }

    /**
     * Add a new value to this bucket. 
     * This method is not implemented. 
     *
     * @param value Table value to add to bucket
     */
    @Override public void add(final TableValue value) { 
	// Not implemented. 
    }

    /**
     * Bulk add the values supplied into this bucket. 
     * This method is not implemented. 
     *
     * @param values Tables values to add to bucket
     */
    @Override public void addAll(final Collection<TableValue> values) {
	// Not implemented. 
    }

    /**
     * Used to iterate over all the values across all the branches. 
     *
     * @param pruneOptions Options to restrict viewing particular values (e.g., deletes)
     * @return Iterator over table values
     */
    @Override public Map<String,StreamIterator<TableValue>> getComplete(final PruneOptions pruneOptions) {
	Map<String,StreamIterator<TableValue>> fm = 
	    new HashMap<>();

	for(String b : entries.keySet()) {
	    fm.putAll(getUncollapsed(b, pruneOptions));
	}

	return fm;
    }

    /**
     * Return the historical list of values from the supplied branch name. 
     *
     * @param branch Specific branch where the value resides
     * @param pruneOptions Options to restrict viewing particular values (e.g., deletes)
     * @return Iterator over table values
     */
    @Override public Map<String,StreamIterator<TableValue>> getUncollapsed(final String branch, 
									   final PruneOptions pruneOptions) {
	Map<String,StreamIterator<TableValue>> fm = 
	    new HashMap<>();

	if(pruneOptions != null) {
	    pruneOptions.delete = 
		hasDeleteOp &&
		pruneOptions.delete;
	}

	TableValueHistory history = entries.get(branch);	
	if(history != null) {
	    TableValueHistory pruned = 
		new UnsafeTableValueHistory(history, pruneOptions);

	    fm.put(branch, pruned.iterator());
	}

	return fm;	
    }

    /**
     * Iterate over the set of independent values. Each independent value 
     * is collapsed so that only the latest value is returned. 
     * 
     * @return Iterator over table values
     */
    @Override public Map<String,StreamIterator<TableValue>> getCollapsed() {
	Map<String,StreamIterator<TableValue>> fm = 
	    new HashMap<>();

	for(String branch : entries.keySet()) {
	    TableValueHistory history = entries.get(branch);
	    TableValue v = history.getCollapsedValue();
	    TableValueHistory singleton = new UnsafeTableValueHistory();
	    singleton.add(v);

	    fm.put(branch, singleton.iterator());
	}

	return fm;
    }

    /**
     * Return the latest value associated with this branch. 
     *
     * @param branch Specific branch where the value resides
     * @return Table value
     */
    @Override public Map<String,StreamIterator<TableValue>> getCollapsed(final String branch) {
	Map<String,StreamIterator<TableValue>> fm = 
	    new HashMap<>(1);

	TableValueHistory history = entries.get(branch);
	if(history != null) {
	    TableValueHistory singleton = new UnsafeTableValueHistory();
	    TableValue v = history.getCollapsedValue();
	    singleton.add(v);

	    fm.put(branch, singleton.iterator());
	}

	return fm;
    }

    /**
     * Return the values that were recorded before or during the supplied time. 
     *
     * @param time Last time recorded
     * @return Iterator over table values
     */
    @Override public Map<String,StreamIterator<TableValue>> getCollapsed(final long time) {
	Map<String,StreamIterator<TableValue>> fm = 
	    new HashMap<>(1);

	for(String branch : entries.keySet()) {
	    TableValueHistory history = entries.get(branch);

	    // Go through the branch and prune off all
	    // the values with higher vector clocks. 
	    TableValueHistory pruned = new UnsafeTableValueHistory();
	    for(Iterator<? extends TableValue> iter = history.iterator();
		iter.hasNext(); ) {
		TableValue v = iter.next();
		long c = v.getClock().getLocalTime();

		// Only include items that were recorded before
		// the supplied time. 		
		if(c <= time) {
		    pruned.add(v);
		}
	    }
		    
	    // Then collapse the remaining values and place
	    // into its own history. 
	    TableValue v = pruned.getCollapsedValue();
	    TableValueHistory singleton = new UnsafeTableValueHistory();
	    singleton.add(v);

	    // Save the new branch. 
	    fm.put(branch, singleton.iterator());
	}

	return fm;
    }

    /**
     * Lock the bucket to all writes.
     */
    @Override public void lockBucket() {
	// Not implemented.
    }

    /**
     * Unlock the bucket to all writes.
     */
    @Override public void unlockBucket() {
	// Not implemented.
    }

    /**
     * Return number of entries in the bucket. Useful to check if additional
     * items have been added after compaction/flushing. 
     *
     * @return Number of bucket elements
     */
    @Override public long size() {
	return num;
    }

    /**
     * Get the memory usage of the entire bucket.
     *
     * @return Number of bytes used by the bucket
     */
    @Override public long memory() {
	long s = 0;

	for(String b : entries.keySet()) {
	    TableValueHistory h = entries.get(b);
	    s += b.length() + h.memory();
	}

	return s;
    }

    /**
     * Return the vector clock for this bucket. The clock is a merger
     * of all the subclocks. 
     *
     * @return Merged vector clock
     */
    @Override public VectorClock getClock() {
	return null;
    }

    /**
     * Return an iterator of vector clocks from each of the data values
     * in the bucket.
     *
     * @return Iterator over vector clocks 
     */
    @Override public Iterator<VectorClock> getAllClocks() {
	return null;
    }

    /**
     * Get all the data associated with this bucket. 
     *
     * @return Serialized data
     */
    @Override public byte[] getData() {
	return null;
    }
}