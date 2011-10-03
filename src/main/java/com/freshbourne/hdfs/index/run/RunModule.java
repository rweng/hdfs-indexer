package com.freshbourne.hdfs.index.run;

import com.freshbourne.hdfs.index.CSVIndex;
import com.freshbourne.hdfs.index.Index;
import com.freshbourne.multimap.btree.BTreeModule;
import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

import java.io.File;
import java.io.Serializable;

public class RunModule extends AbstractModule implements Serializable {

    private String hdfsFile;
    private String indexFolder = "/tmp/index";

    public void setHdfsFile(String s){
        hdfsFile = s;
    }

    // empty constructor is required by IndexBuilder
    public RunModule(){}

    public RunModule(String hdfsFile){
        this.hdfsFile = hdfsFile;
    }

    @Override
    protected void configure() {
        bind(Index.class).to(CSVIndex.class);

        bind(String.class).annotatedWith(Names.named("hdfsFile")).toInstance(hdfsFile);
        bind(File.class).annotatedWith(Names.named("indexFolder")).toInstance(new File(indexFolder));

        bind(Integer.class).annotatedWith(Names.named("csvColumn")).toInstance(3); //L_LINENUMBER
        bind(String.class).annotatedWith(Names.named("delimiter")).toInstance("|");

        install(new BTreeModule());
    }

    public void setIndexFolder(String s) {
        this.indexFolder = s;
    }
}
