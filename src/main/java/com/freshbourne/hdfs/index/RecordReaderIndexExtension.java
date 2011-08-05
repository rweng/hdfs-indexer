package com.freshbourne.hdfs.index;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;

public class RecordReaderIndexExtension {
	private static final Log LOG = LogFactory.getLog(RecordReaderIndexExtension.class);
	private String[] indexFiles;
	private String hdfsPath;
	private boolean doneReadingFromIndex = false;
	private Iterator<String> indexIterator;
	private int indexFilesPointer = 0;
	private Class<?> indexClassName;
	private Index<String, String> index;
	private Properties properties;
	private Configuration conf;
	private long pos;


	public RecordReaderIndexExtension(InputSplit genericSplit,
									  TaskAttemptContext context) {
		// create the index
		conf = context.getConfiguration();
		hdfsPath = inputToFileSplit(genericSplit).getPath().toString();

		File dir = new File(getIndexFolder());
		dir.mkdirs();

		String indexFolder = dir.getAbsolutePath() + "/";
		fillIndexFilesArray(dir);
		loadProperties();


		// otherwise load index
		indexClassName = conf.getClass("Index", null);
		if (indexClassName == null)
			 throw new IllegalArgumentException(
					   "Index class must be set in config");


	}

	private void loadProperties() {
		// load the properties
          properties = new Properties();
		File propertiesFile = new File(getIndexFolder() + "properties");
          if(propertiesFile.exists()){
               try{
                    FileInputStream fis = new FileInputStream(propertiesFile);
                    properties.loadFromXML(fis);
               } catch (Exception e) {
                    LOG.debug("deleting properties file");
                    propertiesFile.delete();
               }
          }

          if(properties == null){
               throw new IllegalStateException("properties should not be null");
          }
	}

	private void fillIndexFilesArray(File dir) { // fill index Files array
		final String fileName = (new File(hdfsPath)).getName();
		if (dir.exists()) {
			LOG.debug("dir exists: " + dir.getAbsolutePath());
			FilenameFilter filter = new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					if (name.startsWith(fileName))
						return true;
					return false;
				}
			};

			// getting files in the right order
			String[] tmpIndexFiles = dir.list(filter);
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
			LOG.debug("dir doesn't exist: " + dir.getAbsolutePath());
			indexFiles = new String[0];
		}

		LOG.debug("indexFiles: ");
		for (String s : indexFiles)
			LOG.debug("indexFile: " + s);
	}

	protected String indexFolder;
	private String getIndexFolder() {
		if(indexFolder != null)
			return indexFolder;
		
		String file = generateIndexPath(conf.get("indexSavePath"), "", "");
		int index = file.lastIndexOf('/');
		indexFolder = file.substring(0, index);
		LOG.debug("generated index folder: " + indexFolder);
		return indexFolder;
	}


	private String generateIndexPath(String folder, String startPos, String columnIdentifier) {

		File folderFile = (new File(folder));
		if (!(folderFile.isDirectory() || !folderFile.exists()))
			throw new IllegalArgumentException("savePath must be a folder: "
					+ folderFile.getAbsolutePath());

		if (!hdfsPath.startsWith("hdfs://")) {
			throw new IllegalArgumentException(
					"The File for the Index must be in the hdfs");
		}

		String path = hdfsPath.replaceFirst("hdfs://[^\\/]+", "");
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

	private static FileSplit inputToFileSplit(InputSplit inputSplit) {
		FileSplit split;
		try {
			split = (FileSplit) inputSplit;
		} catch (Exception e) {
			throw new IllegalArgumentException(
					"InputSplit must be an instance of FileSplit");
		}
		return split;
	}

	public String getNextFromIndex() {
		LOG.debug("getNextFromIndex()");
          if(doneReadingFromIndex)
               return null;
         
          LOG.debug("not done");
         
          // return next if index and iterator are loaded
          if(indexIterator != null && indexIterator.hasNext()){
               LOG.debug("returning next from index");
               return indexIterator.next();
          }

		// return null if there is no more index
          if(indexFilesPointer >= indexFiles.length){
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
               if(!properties.containsKey(indexFiles[indexFilesPointer - 1])){
                    File f = new File(indexFolder + indexFiles[indexFilesPointer - 1]);
                    LOG.debug("deleting file: " + f.getAbsolutePath());
                    f.delete();
                    index = null;
                    indexIterator = null;
                    LOG.debug("no data of this index partial in the properties file: " + indexFiles[indexFilesPointer - 1]);
                    throw new IllegalStateException("no data of this index partial in the properties file");
               }

               String offset = properties.getProperty(indexFiles[indexFilesPointer - 1]);

               LOG.debug("Loading index " + indexFolder + indexFiles[indexFilesPointer - 1]);
               index.initialize(indexFolder + indexFiles[indexFilesPointer - 1]);
               indexIterator = index.getIterator();

               LOG.debug("Adjusting POS: from " + pos + " to " + offset);
               pos = Long.parseLong(offset);
          } catch (Exception e) {
               LOG.debug("exception in loading index: " + e.getMessage());
               index = null;
               indexIterator = null;
          }

          return getNextFromIndex();
	}

	public long getPos() {
		return pos;
	}
}
