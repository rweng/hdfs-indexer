package com.freshbourne.hdfs.index;

import com.freshbourne.hdfs.index.run.RunModule;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * this class is only used to create indexes. Maybe we should make the Index interface an
 * abstract class and put this method in there?
 */
public class IndexBuilder {
    private static Logger LOG = Logger.getLogger(IndexBuilder.class);
    public static Index create(InputSplit genericSplit, TaskAttemptContext context) {
        LOG.setLevel(Level.DEBUG);

        Configuration conf = context.getConfiguration();
        
        Class<?> indexClass = conf.getClass("Index", null);
        Class<?> guiceModule = conf.getClass("GuiceModule", null);

        String hdfsFile = inputToFileSplit(genericSplit).getPath().toString();
        LOG.debug("HDFS-FILE: " + hdfsFile);

        try {
            LOG.debug("trying to create index of class "  + indexClass.toString() + " with module " + guiceModule.toString());
            RunModule module = (RunModule) guiceModule.getConstructor().newInstance();
            module.setHdfsFile(hdfsFile);
            LOG.debug("module created: " + module.getClass().getName().toString());
            Injector injector = Guice.createInjector(module);
            LOG.debug("injector created");
            Index index = (Index) injector.getInstance(indexClass);
            index.open();
            return index;
        } catch (Exception e) {
            LOG.debug(e);
            return null;
        }
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

}
