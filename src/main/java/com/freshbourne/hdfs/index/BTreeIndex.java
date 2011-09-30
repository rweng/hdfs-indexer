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
        } catch (IOException e){
            saveProperties();
        }

        return properties;
    }

    public boolean open() throws Exception {
        File indexDir = getIndexDir();
        indexDir.mkdirs();

        loadOrCreateProperties();

        isOpen = true;
        return true;
    }

    /**
     * @return directory of the index-files for the current hdfs file
     */
    File getIndexDir() {
        return new File(indexRootFolder.getPath() + hdfsFile);
    }

    class BTreeIndexIterator implements Iterator<String> {

        private List<BTree<String, String>> trees;
        private BTree<String, String> currentTree;
        private Iterator<String> currentIterator;
        
        /**
         * @param trees ordered list of btrees from which iterators are used
         */
        private BTreeIndexIterator(List<BTree<String, String>> trees){
            this.trees = trees;
        }

        @Override
        public boolean hasNext() {
            if(trees.size() == 0){
                return false;
            }

            // initial tree
            if(currentTree == null)
                currentTree = trees.get(0);

            if(currentIterator == null){
                currentIterator = currentTree.getIterator();
            }

            if(currentIterator.hasNext()){
                return true;
            }

            // try to get next tree
            int nextTree = trees.indexOf(currentTree) + 1;
            if(nextTree >= trees.size()){
                return false;
            } else {
                currentTree = trees.get(nextTree);
            }

            return hasNext();
        }

        @Override
        public String next() {
            if(hasNext()){
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

    private List<BTree<String, String>> getTreeList(){
        List<BTree<String, String>> list = new LinkedList<BTree<String, String>>();

        

        if(bTreeWriting != null)
            list.add(bTreeWriting);

        return list;
    }

    @Override
    public Iterator<String> getIterator() {
        return new BTreeIndexIterator(getTreeList());
    }

    @Override
    public Iterator<AbstractMap.SimpleEntry<String, String>> getIterator(String start, String end) {
        //TODO: implement
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        try {
            saveProperties();
        } catch (IOException e) {
            // we dont need to do the bTree writing if this fails
            throw new RuntimeException(e);
        }

        if (bTreeWriting != null)
            bTreeWriting.sync();

    }

    @Override
    public void addLine(String line, long pos) {
        String key = extractKeyFromLine(line);
        String[] splits = getOrCreateWritingTree().getPath().split("/");
        String startProp =  splits[splits.length - 1] + "_start";

        splits = getOrCreateWritingTree().getPath().split("/");
        String endProp = splits[splits.length - 1] + "_end";
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

        String file = getIndexDir() + "/" + indexId + "_" + (new SecureRandom()).nextInt();
        
        try {
            bTreeWriting = factory.get(new File(file), FixedStringSerializer.INSTANCE, FixedStringSerializer.INSTANCE,
                    StringComparator.INSTANCE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

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
