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

/**
 * This is the base class for all Indexes using the multimap btree.
 *
 * An instance can be created without any parameters to the constructor,
 * but to ensure that it is working initialize must be called!
 * The reason for not using the constructor are several:
 * First, constructor inheritance isn't supported so for subclasses the constructor would have to be repeated.
 * Second, in a constructor, this() has to be the first statement. This forces parsing to be done directly
 * in the parameter which results in method calls like this:
 *
 * this(new File(inputSplitToFileSplit(genericSplit).getPath().toString()).getName(),
 *               indexId,
 *               new File(context.getConfiguration().get("indexSavePath")));
 *
 *
 * 
 * Also, to ensure that data is really written to the disk, close has to be called as specified by the
 * Index interface.
 *
 * The instance gets a path in which all btrees are stored.
 * The folder structure within this index-path is acording to the files of the HDFS.
 *
 * Example:
 * If the index-path is /data/indexes, and an index is created over the second column of hdfs:///csvs/users.csv,
 * the index files that are created are:
 *
 * /data/indexes/csvs/users.csv/properties.xml
 * /data/indexes/csvs/users.csv/2_0
 * /data/indexes/csvs/users.csv/2_10005
 *
 * With each btree, there are four information that have to be stored:
 *
 * - the hdfs file name and path
 * - the identifier to be indexed (column, xml-path, ...)
 * - the starting position in the hdfs file for the index
 * - the end position in the hdfs file for the index
 *
 * The file name and path are stored in the structure in the index directory.
 * The identifier and starting position could be stored directly in the file name.
 * However, as it is unsure where the indexing will end at the time the index file is created,
 * it is not possible to store the end position in the file name (assuming we dont want to rename).
 * Thus, a properties file is required.
 *
 */
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
