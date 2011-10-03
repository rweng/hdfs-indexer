package com.freshbourne.hdfs.index;

import com.freshbourne.btree.BTree;
import com.freshbourne.btree.BTreeFactory;
import com.freshbourne.comparator.StringComparator;
import com.freshbourne.serializer.FixedStringSerializer;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.*;
import java.security.SecureRandom;
import java.util.*;

/**
 * This is the base class for all Indexes using the multimap bTreeWriting.
 * <p/>
 * Instances should be created through Guice.
 * Reason for this are manifold.
 * <p/>
 * The folder structure and files are not created when creating an instance of this class.
 * Rather, you need to open the class explicitly through the open() method.
 * <p/>
 * Also, to ensure that data is really written to the disk, close has to be called as specified by the
 * Index interface.
 * <p/>
 * Some methods, like exists() work without an opened instance.
 * Most methods, however, require that the instance was opened first.
 * <p/>
 * The instance gets a path in which all btrees are stored.
 * The folder structure within this index-path is acording to the files of the HDFS.
 * <p/>
 * Example:
 * If the index-path is /data/indexes, and an index is created over the second column of hdfs:///csvs/users.csv,
 * the index files that are created are:
 * <p/>
 * /data/indexes/csvs/users.csv/properties.xml
 * /data/indexes/csvs/users.csv/2_0
 * /data/indexes/csvs/users.csv/2_10005
 * <p/>
 * With each bTreeWriting, there are four information that have to be stored:
 * <p/>
 * - the hdfs file name and path
 * - the identifier to be indexed (column, xml-path, ...)
 * - the starting position in the hdfs file for the index
 * - the end position in the hdfs file for the index
 * <p/>
 * The file name and path are stored in the structure in the index directory.
 * The identifier and starting position could be stored directly in the file name.
 * However, as it is unsure where the indexing will end at the time the index file is created,
 * it is not possible to store the end position in the file name (assuming we dont want to rename).
 * Thus, a properties file is required.
 */
public abstract class BTreeIndex implements Index, Serializable {

    private static final long serialVersionUID = 1L;
    private BTree<String, String> bTreeWriting;
    private static final Logger LOG = Logger.getLogger(BTreeIndex.class);

    private String     indexId;
    private String     hdfsFile;
    private File       indexRootFolder;
    private Properties properties;

    private boolean isOpen = false;
    private BTreeFactory factory;

    public boolean isOpen() {
        return isOpen;
    }

    String getPropertiesPath() {
        return getIndexDir() + "/properties.xml";
    }

    private void saveProperties() throws IOException {
        properties.storeToXML(new FileOutputStream(getPropertiesPath()), "comment");
    }

    private Properties loadOrCreateProperties() throws IOException {
        properties = new Properties();

        try {
            properties.loadFromXML(new FileInputStream(getPropertiesPath()));
        } catch (IOException e) {
            saveProperties();
        }

        return properties;
    }

    public void open() throws IOException {
        File indexDir = getIndexDir();
        indexDir.mkdirs();

        loadOrCreateProperties();

        isOpen = true;
    }

    public class PropertyEntry {
        private long start;
        private long end;

        public PropertyEntry() {
            this(Long.MAX_VALUE, -1);
        }

        public PropertyEntry(long start, long end) {
            this.end = end;
            this.start = start;
        }

        public String toString() {
            return "" + start + ";" + end;
        }

        public void loadFromString(String s) {
            String[] splits = s.split(";");
            start = Long.parseLong(splits[0]);
            end = Long.parseLong(splits[1]);
        }
    }

    /**
     * @return directory of the index-files for the current hdfs file
     */
    File getIndexDir() {
        return new File(indexRootFolder.getPath() + hdfsFile);
    }

    class BTreeIndexIterator implements Iterator<String> {

        private List<BTree<String, String>> trees;
        private BTree<String, String>       currentTree;
        private Iterator<String>            currentIterator;

        /**
         * @param trees ordered list of btrees from which iterators are used
         */
        private BTreeIndexIterator(List<BTree<String, String>> trees) {
            this.trees = trees;
        }

        @Override
        public boolean hasNext() {
            if (trees.size() == 0) {
                return false;
            }

            // initial tree
            if (currentTree == null)
                currentTree = trees.get(0);

            if (currentIterator == null) {
                currentIterator = currentTree.getIterator();
            }

            if (currentIterator.hasNext()) {
                return true;
            }

            // try to get next tree
            int nextTree = trees.indexOf(currentTree) + 1;
            if (nextTree >= trees.size()) {
                return false;
            } else {
                currentTree = trees.get(nextTree);
            }

            return hasNext();
        }

        @Override
        public String next() {
            if (hasNext()) {
                return currentIterator.next();
            }

            return null;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    @Inject
    protected BTreeIndex(@Named("hdfsFile") String hdfsFile, @Named("indexFolder") File indexFolder,
                         @Named("indexId") String indexId, BTreeFactory factory) {

        // if hdfsFile doesn't start with /, the server name is before the path
        hdfsFile = hdfsFile.replaceAll("^hdfs://[^/]*", "");

        LOG.setLevel(Level.DEBUG);
        LOG.debug("parsed hdfs file: " + hdfsFile);

        this.hdfsFile = hdfsFile;
        this.indexRootFolder = indexFolder;
        this.indexId = indexId;
        this.factory = factory;
    }

    String getHdfsFile() {
        return hdfsFile;
    }

    private Properties getProperties() {
        if (properties != null) {
            return properties;
        }

        properties = new Properties();
        File propertiesFile = new File(indexRootFolder + "properties");
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

    private List<BTree<String, String>> getTreeList() {
        List<BTree<String, String>> list = new LinkedList<BTree<String, String>>();

        // add trees from properties
        for (String filename : getProperties().stringPropertyNames()) {
            list.add(getTree(new File(getIndexDir() + "/" + filename)));
        }


        return list;
    }

    @Override
    public Iterator<String> getIterator() {
        ensureOpen();
        return new BTreeIndexIterator(getTreeList());
    }

    protected void ensureOpen() {
        if(!isOpen())
            throw new IllegalStateException("index must be opened before it is used");
    }

    @Override
    public Iterator<AbstractMap.SimpleEntry<String, String>> getIterator(String start, String end) {
        ensureOpen();
        //TODO: implement
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        if(!isOpen())
            return;
        
        try {
            saveProperties();
        } catch (IOException e) {
            // we dont need to do the bTree writing if this fails
            throw new RuntimeException(e);
        }

        if (bTreeWriting != null)
            bTreeWriting.sync();

    }

    private String getWriteTreeFileName() {
        if (bTreeWriting == null)
            return null;

        String[] splits = getOrCreateWritingTree().getPath().split("/");
        return splits[splits.length - 1];
    }

    @Override
    public void addLine(String line, long pos) {
        ensureOpen();
        String key = extractKeyFromLine(line);
        getOrCreateWritingTree().add(key, line);

        String filename = getWriteTreeFileName();
        String propertyStr = properties.getProperty(filename, null);
        PropertyEntry p = new PropertyEntry();

        if (propertyStr != null)
            p.loadFromString(propertyStr);

        if (pos < p.start) {
            p.start = pos;
        }

        if (pos > p.end) {
            p.end = pos;
        }

        properties.setProperty(filename, p.toString());
    }


    private BTree<String, String> getOrCreateWritingTree() {
        if (bTreeWriting != null)
            return bTreeWriting;

        String file = getIndexDir() + "/" + indexId + "_" + (new SecureRandom()).nextInt();

        bTreeWriting = getTree(new File(file));
        return bTreeWriting;
    }

    private BTree<String, String> getTree(File file) {
        BTree<String, String> result = null;
        try {
            result = factory.get(file, FixedStringSerializer.INSTANCE_1000, FixedStringSerializer.INSTANCE_1000,
                    StringComparator.INSTANCE);
        } catch (IOException e) {
            throw new RuntimeException("error occured while trying to get btree: " + file.getAbsolutePath(), e);
        }
        return result;
    }


    @Override
    public boolean exists() {
        throw new UnsupportedOperationException("todo: check if directory and properties file exists");
    }


    /**
     * This method implemented by a subclass returns the key for a given line.
     * <p/>
     * This method isn't perfect since it assumes that each line is one entry.
     * Maybe this can be made more generic later!
     *
     * Also, call ensureOpen() in this method
     *
     * @param line in the hdfs file
     * @return key or null to ignore the line
     */
    public abstract String extractKeyFromLine(String line);
}
