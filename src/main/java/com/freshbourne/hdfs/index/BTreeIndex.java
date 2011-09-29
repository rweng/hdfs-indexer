package com.freshbourne.hdfs.index;

import com.freshbourne.multimap.btree.BTree;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Properties;

/**
 * This is the base class for all Indexes using the multimap btree.
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
 * With each btree, there are four information that have to be stored:
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
public class BTreeIndex implements Index, Serializable {

    private static final long serialVersionUID = 1L;
    private BTree<String, String> btree;
    private static final Logger LOG = Logger.getLogger(BTreeIndex.class);

    private String     indexId;
    private String     hdfsFile;
    private File       indexRootFolder;
    private Properties properties;
    
    private boolean isOpen = false;

    public boolean isOpen() {
        return isOpen;
    }

    String getPropertiesPath(){
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
    protected BTreeIndex(@Named("hdfsFile") String hdfsFile, @Named("indexFolder") File indexFolder, @Named("indexId") String indexId) {
        hdfsFile = hdfsFile.replaceAll("^hdfs://", "");

        this.hdfsFile = hdfsFile;
        this.indexRootFolder = indexFolder;
        this.indexId = indexId;
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
