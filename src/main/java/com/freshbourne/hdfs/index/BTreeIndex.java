package com.freshbourne.hdfs.index;

import com.freshbourne.comparator.StringComparator;
import com.freshbourne.multimap.btree.BTree;
import com.freshbourne.multimap.btree.BTreeFactory;
import com.freshbourne.serializer.FixedStringSerializer;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.log4j.Logger;

import java.io.*;
import java.security.SecureRandom;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Properties;

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

    public boolean open() throws Exception {
        File indexDir = getIndexDir();
        indexDir.mkdirs();

        properties = new Properties();
        properties.storeToXML(new FileOutputStream(getPropertiesPath()), "comment");

        isOpen = true;
        return true;
    }

    /**
     * @return directory of the index-files for the current hdfs file
     */
    File getIndexDir() {
        return new File(indexRootFolder.getPath() + hdfsFile);
    }

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

    @Inject
    protected BTreeIndex(@Named("hdfsFile") String hdfsFile, @Named("indexFolder") File indexFolder,
                         @Named("indexId") String indexId, BTreeFactory factory) {
        
        hdfsFile = hdfsFile.replaceAll("^hdfs://", "");

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

    @Override
    public Iterator<AbstractMap.SimpleEntry<String, String>> getIterator() {
        return new BTreeIndexIterator();
    }

    @Override
    public Iterator<AbstractMap.SimpleEntry<String, String>> getIterator(String start, String end) {
        //TODO: implement
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        if (bTreeWriting != null)
            bTreeWriting.sync();
    }

    @Override
    public void addLine(String line, long pos) {
        String key = extractKeyFromLine(line);
        String startProp = bTreeWriting.getPath() + "_start";
        String endProp = bTreeWriting.getPath() + "_end";
        getOrCreateWritingTree().add(key, line);
        long start = Long.parseLong(properties.getProperty(startProp, "" + Long.MAX_VALUE));
        long end = Long.parseLong(properties.getProperty(endProp, "" + -1));

        if(pos < start){
            properties.setProperty(startProp, "" + pos);
        }

        if(pos > end){
            properties.setProperty(endProp, "" + pos);
        }
    }


    private BTree<String, String> getOrCreateWritingTree() {
        if(bTreeWriting != null)
            return bTreeWriting;

        String file = getIndexDir() + indexId + (new SecureRandom()).nextInt();
        bTreeWriting = factory.get(new File(file), FixedStringSerializer.INSTANCE, FixedStringSerializer.INSTANCE,
                StringComparator.INSTANCE);
        
        return bTreeWriting;
    }

    /**
     * This method implemented by a subclass returns the key for a given line.
     *
     * This method isn't perfect since it assumes that each line is one entry.
     * Maybe this can be made more generic later!
     *
     * @param line in the hdfs file
     * @return key or null to ignore the line
     */
    public abstract String extractKeyFromLine(String line);
}
