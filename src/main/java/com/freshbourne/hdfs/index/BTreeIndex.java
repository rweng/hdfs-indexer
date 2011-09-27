package com.freshbourne.hdfs.index;

import com.freshbourne.multimap.btree.BTree;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Properties;

public class BTreeIndex implements Index, Serializable {

    private static final long serialVersionUID = 1L;
    protected BTree<String, String> btree;
    protected static final Logger LOG = Logger.getLogger(CSVIndex.class);
    private String indexId;
    private String hdfsFile;
    private File indexFolder;
    private Properties properties;


    class BTreeIndexIterator implements Iterator<AbstractMap.SimpleEntry<String, String>> {

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public AbstractMap.SimpleEntry<String, String> next() {
            return null;
        }

        @Override
        public void remove() {

        }
    }

    public void initialize(InputSplit genericSplit, TaskAttemptContext context, String indexId) {
        // because this() has to be the first statement in the constructor, ...
        initialize(new File(inputSplitToFileSplit(genericSplit).getPath().toString()).getName(),
                indexId,
                new File(context.getConfiguration().get("indexSavePath")));
    }

    /**
     *
     * @param hdfsFile the file in the hdfs
     * @param indexId like the column or the jquery-path
     * @param indexFolder for all indexes. This is not the subFolder within the indexFolder
     */
    //TODO: ensure that this methods has been called before any other method.
    public void initialize(String hdfsFile, String indexId, File indexFolder) {
        if (hdfsFile == "" || hdfsFile == null) {
            throw new IllegalArgumentException("hdfsFile must be set and not empty");
        }

        if (indexId == "" || indexId == null) {
            throw new IllegalArgumentException("indexId must be set and not empty");
        }

        if (indexFolder == null || !indexFolder.isDirectory()) {
            throw new IllegalArgumentException("indexFolder must be set and exist");
        }


        this.hdfsFile = hdfsFile;
        this.indexId = indexId;
        this.indexFolder = indexFolder;
    }

    private Properties getProperties() {
        if (properties != null) {
            return properties;
        }

        properties = new Properties();
        File propertiesFile = new File(indexFolder + "properties");
        if (propertiesFile.exists()) {
            try {
                FileInputStream fis = new FileInputStream(propertiesFile);
                properties.loadFromXML(fis);
            } catch (Exception e) {
                LOG.debug("deleting properties file");
                propertiesFile.delete();
            }
        }

        return properties;
    }

    @Override
    public Iterator<AbstractMap.SimpleEntry<String, String>> getIterator() {
        return new BTreeIndexIterator();
    }

    @Override
    public Iterator<AbstractMap.SimpleEntry<String, String>> getIterator(String start, String end) {
        // TODO: implement
        return new BTreeIndexIterator();
    }

    @Override
    public void close() {
        if (btree != null)
            btree.sync();
    }

    @Override
    public void addLine(String line, long pos) {
        // TODO: implement
    }


    private static FileSplit inputSplitToFileSplit(InputSplit inputSplit) {
        FileSplit split;
        try {
            split = (FileSplit) inputSplit;
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "InputSplit must be an instance of FileSplit");
        }
        return split;
    }

    /**
     *
     * generates the complete path to the indexes
     *
     * @param folder
     * @param startPos
     * @param columnIdentifier
     * @return
     */
    /*
    private String generateIndexPath(String folder, String startPos, String columnIdentifier) {

        File folderFile = (new File(folder));
        if (!(folderFile.isDirectory() || !folderFile.exists()))
            throw new IllegalArgumentException("savePath must be a folder: "
                    + folderFile.getAbsolutePath());

        if (!processedHdfsFile.startsWith("hdfs://")) {
            throw new IllegalArgumentException(
                    "The File for the Index must be in the hdfs");
        }

        String path = processedHdfsFile.replaceFirst("hdfs://[^\\/]+", "");
        String[] splits = path.split("\\/");
        String fileName = splits[splits.length - 1];
        LOG.debug("path: " + path);
        String path2 = folderFile.getAbsolutePath() + path;

        String path3 = path2 + "_" + columnIdentifier + "_"
                + startPos;
        LOG.debug("FILE NAME: " + path3);
        (new File(path3).getParentFile()).mkdirs();

        return path3;
    }
    */
}
