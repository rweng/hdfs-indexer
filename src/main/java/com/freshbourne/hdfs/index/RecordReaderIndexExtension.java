package com.freshbourne.hdfs.index;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;


/**
 * extends the RecordReader to read and write from and to an index
 */
public class RecordReaderIndexExtension {
    private static final Log LOG = LogFactory.getLog(RecordReaderIndexExtension.class);
    private String processedHdfsFile;
    private boolean doneReadingFromIndex = false;
    private Iterator<String> indexIterator;
    private int indexFilesPointer = 0;
    private Class<?> indexClassName;
    private Index<String, String> index;
    private Properties properties;
    private Configuration conf;
    private long pos;
    private String value;
    private SharedContainer container;
    private IndexedRecordReader baseReader;
    private TaskAttemptContext context;
    private InputSplit inputSplit;


    private String[] indexFiles;

    protected String[] getIndexFiles() {
        if (indexFiles != null)
            return indexFiles;

        final String fileName = (new File(processedHdfsFile)).getName();

        if (getIndexFolder().exists()) {
            LOG.debug("dir exists: " + getIndexFolder().getAbsolutePath());
            FilenameFilter filter = new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    if (name.startsWith(fileName))
                        return true;
                    return false;
                }
            };

            // getting files in the right order
            String[] tmpIndexFiles = getIndexFolder().list(filter);
            indexFiles = new String[tmpIndexFiles.length];
            HashMap<Integer, String> map = new HashMap<Integer, String>();
            for (String name : tmpIndexFiles) {
                map.put(Integer.parseInt(name.split("_")[2]), name);
            }

            Integer[] keys = new Integer[map.keySet().size()];
            map.keySet().toArray(keys);
            java.util.Arrays.sort(keys);
            for (int i = 0; i < keys.length; i++)
                indexFiles[i] = map.get(keys[i]);

        } else {
            LOG.debug("dir doesn't exist: " + getIndexFolder().getAbsolutePath());
            indexFiles = new String[0];
        }

        LOG.debug("indexFiles: ");
        for (String s : indexFiles)
            LOG.debug("indexFile: " + s);

        return indexFiles;
    }


    public RecordReaderIndexExtension(IndexedRecordReader base, InputSplit genericSplit,
                                      TaskAttemptContext context) {
        conf = context.getConfiguration();

        this.baseReader = base;
        this.context = context;
        this.inputSplit = genericSplit;
        this.indexClassName = conf.getClass("Index", null);
        processedHdfsFile = inputSplitToFileSplit(genericSplit).getPath().toString();

        /*
		// create the index

		File dir = new File(getIndexFolder());

		String indexFolder = dir.getAbsolutePath() + "/";
		fillIndexFilesArray(dir);
		loadProperties();

		// otherwise load the indexClassName.
        // the index itself is created later when fetching or
        // adding values
		if (indexClassName == null)
			 throw new IllegalArgumentException(
					   "Index class must be set in config");


        // create the index to be passed to the container

        */

        // container = new SharedContainer<String, String>(index);
    }

    private void loadProperties() {
        // load the properties
        properties = new Properties();
        File propertiesFile = new File(getIndexFolder() + "properties");
        if (propertiesFile.exists()) {
            try {
                FileInputStream fis = new FileInputStream(propertiesFile);
                properties.loadFromXML(fis);
            } catch (Exception e) {
                LOG.debug("deleting properties file");
                propertiesFile.delete();
            }
        }

        if (properties == null) {
            throw new IllegalStateException("properties should not be null");
        }
    }

    protected File indexFolder;

    protected File getIndexFolder() {
        if (indexFolder != null)
            return indexFolder;

        String file = generateIndexPath(conf.get("indexSavePath"), "", "");
        int index = file.lastIndexOf('/');
        String indexFolderPath = file.substring(0, index);
        LOG.debug("generated index folder: " + indexFolderPath);

        File dir = new File(indexFolderPath);

        return dir;
    }


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

    public String nextFromIndex() {
        LOG.debug("nextFromIndex()");
        if (doneReadingFromIndex)
            return null;

        LOG.debug("not done reading from index");

        // return next if index and iterator are loaded
        if (indexIterator != null && indexIterator.hasNext()) {
            LOG.debug("returning next from index");
            value = (indexIterator.next());
            return value;
        }

        // return null if there is no more index
        if (indexFilesPointer >= getIndexFiles().length) {
            doneReadingFromIndex = true;
            index = null;
            indexIterator = null;
            return null;
        }

        LOG.debug("didnt return null, loading index");

        try {
            index = (Index<String, String>) (indexClassName.getConstructor().newInstance());

            // try to load the index
            indexFilesPointer++;
            LOG.debug("loading offset");
            if (!properties.containsKey(getIndexFiles()[indexFilesPointer - 1])) {
                File f = new File(indexFolder + getIndexFiles()[indexFilesPointer - 1]);
                LOG.debug("deleting file: " + f.getAbsolutePath());
                f.delete();
                index = null;
                indexIterator = null;
                LOG.debug("no data of this index partial in the properties file: " + getIndexFiles()[indexFilesPointer - 1]);
                throw new IllegalStateException("no data of this index partial in the properties file");
            }

            String offset = properties.getProperty(getIndexFiles()[indexFilesPointer - 1]);

            LOG.debug("Loading index " + indexFolder + getIndexFiles()[indexFilesPointer - 1]);
            index.initialize(indexFolder + getIndexFiles()[indexFilesPointer - 1]);
            indexIterator = index.getIterator();

            LOG.debug("Adjusting POS: from " + pos + " to " + offset);
            pos = Long.parseLong(offset);
        } catch (Exception e) {
            LOG.debug("exception in loading index: " + e.getMessage());
            index = null;
            indexIterator = null;
        }

        return nextFromIndex();
    }

    public long getPos() {
        return pos;
    }

    public String getCurrentValue() {
        return value;
    }

    public SharedContainer getSharedContainer() {
        if (container != null) {
            return container;
        }

        try {
            Index<String, String> sharedIndex = (Index<String, String>) (indexClassName.getConstructor().newInstance());
            container = new SharedContainer<String, String>(sharedIndex);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return container;
    }

    /**
     * adds a newly read key value to the index
     *
     * @param currentKey
     * @param currentValue
     */
    public void addKeyValue(LongWritable currentKey, Text currentValue) {


//        if(index == null){
//            try {
//                index = (Index<String, String>) (indexClassName.getConstructor().newInstance());
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }
//
//            String path = generateIndexPath(getIndexFolder(), "" + getPos(), index.getIdentifier());
//            index.initialize(path);
//            LOG.warn("index is null!");
//            return;
//        }
//
//        if(currentValue == null){
//            LOG.warn("currentValue is null!");
//            return;
//        }
//
//        index.parseEntry(currentValue.toString());

        getSharedContainer().add(currentValue.toString(), baseReader.getPos());

        if (getSharedContainer().isFinished())
            (new IndexWriterThread(getSharedContainer())).run();
    }
}
