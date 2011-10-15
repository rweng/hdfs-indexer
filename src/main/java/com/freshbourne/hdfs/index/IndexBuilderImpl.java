package com.freshbourne.hdfs.index;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

/**
 * this class is only used to create indexes. Maybe we should make the Index interface an abstract class and put this
 * method in there?
 */
public class IndexBuilderImpl implements IndexBuilder {
	private static Logger LOG = Logger.getLogger(IndexBuilderImpl.class);

	@Override public Index create(InputSplit genericSplit, TaskAttemptContext context) {

		Configuration conf = context.getConfiguration();

		Class<?> guiceModule = conf.getClass("GuiceModule", null);

		try {
			if (LOG.isDebugEnabled())
				LOG.debug("trying to create index with module " + guiceModule);
			Module module = (Module) guiceModule.getConstructor().newInstance();
			if (LOG.isDebugEnabled())
				LOG.debug("module created: " + module.getClass().getName().toString());
			Injector injector = Guice.createInjector(module);
			if (LOG.isDebugEnabled())
				LOG.debug("injector created");
			Index index = injector.getInstance(Index.class);
			if (LOG.isDebugEnabled())
				LOG.debug("opening index");
			index.open();
			if (LOG.isDebugEnabled())
				LOG.debug("index opened. returning index.");
			return index;
		} catch (Exception e) {
			LOG.warn(e);
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
