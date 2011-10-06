package com.freshbourne.hdfs.index;

import com.freshbourne.btree.BTreeModule;
import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

import java.io.File;
import java.io.Serializable;

public abstract class CSVModule extends AbstractModule implements Serializable {
    @Override protected void configure() {

        bind(String.class).annotatedWith(Names.named("hdfsFile")).toInstance(hdfsFile());
        bind(File.class).annotatedWith(Names.named("indexFolder")).toInstance(new File(indexRootFolder()));

        bind(Integer.class).annotatedWith(Names.named("csvColumn")).toInstance(csvColumn());
	    bind(String.class).annotatedWith(Names.named("indexId")).toInstance("" + csvColumn());
        bind(String.class).annotatedWith(Names.named("delimiter")).toInstance(delimiter());

	    bind(Index.class).to(indexClass());
        install(new BTreeModule());
    }

    protected abstract String hdfsFile();
    protected abstract String indexRootFolder();
    protected abstract int csvColumn();
    protected abstract String delimiter();
	protected abstract Class<? extends Index> indexClass();
}
