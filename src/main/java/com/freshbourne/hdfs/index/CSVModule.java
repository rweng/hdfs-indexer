package com.freshbourne.hdfs.index;

import com.freshbourne.btree.BTreeModule;
import com.freshbourne.btree.Range;
import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

import java.io.File;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

public class CSVModule extends AbstractModule implements Serializable {
    public String hdfsFile = "/test/lineitem.tbl";
	public String indexRootFolder = "/tmp/index";
	public int csvColumn = 0;
	public String delimiter = "( |\t)+";
	public Class<? extends Index> indexClass = IntegerCSVIndex.class;
	public List<Range<Integer>> searchRange = new LinkedList<Range<Integer>>();

	@Override protected void configure() {

        bind(String.class).annotatedWith(Names.named("hdfsFile")).toInstance(hdfsFile);
        bind(File.class).annotatedWith(Names.named("indexFolder")).toInstance(new File(indexRootFolder));

        bind(Integer.class).annotatedWith(Names.named("csvColumn")).toInstance(csvColumn);
	    bind(String.class).annotatedWith(Names.named("indexId")).toInstance("" + csvColumn);
        bind(String.class).annotatedWith(Names.named("delimiter")).toInstance(delimiter);

	    bind(Index.class).to(indexClass);
        install(new BTreeModule());
    }
}
