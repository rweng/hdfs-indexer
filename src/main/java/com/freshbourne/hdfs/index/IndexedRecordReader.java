package com.freshbourne.hdfs.index;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Iterator;

import com.freshbourne.hdfs.index.mapreduce.LineRecordReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class IndexedRecordReader extends LineRecordReader {
    private static final Log LOG = LogFactory.getLog(IndexedRecordReader.class);

    private Index index;
    private Iterator<AbstractMap.SimpleEntry<String, String>> indexIterator;

    public void initialize(InputSplit genericSplit, TaskAttemptContext context)
            throws IOException {
        super.initialize(genericSplit, context);
        index = IndexBuilder.create(genericSplit, context);
    }

    public Iterator<AbstractMap.SimpleEntry<String, String>> getIndexIterator() {
        if (indexIterator == null) {
            indexIterator = index.getIterator();
        }

        return indexIterator;
    }

    public boolean nextKeyValue() throws IOException {
        // get next value from index as long as we have
        if (getIndexIterator().hasNext()) {
            LOG.debug("READING FROM INDEX");

            // get index for file if not set
            // read from index
            AbstractMap.SimpleEntry<String, String> next = getIndexIterator().next();
            if (next != null) {
                value.set(next.getValue());
                return true;
            } else {
                return false;
            }
        }

        boolean result = super.nextKeyValue();

        if (result)
            index.addLine(this.getCurrentValue().toString(), pos);
        else
            index.close();

        return result;
    }
}
