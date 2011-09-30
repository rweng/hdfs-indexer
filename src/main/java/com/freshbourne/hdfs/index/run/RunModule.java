package com.freshbourne.hdfs.index.run;

import com.freshbourne.hdfs.index.CSVIndex;
import com.freshbourne.hdfs.index.Index;
import com.freshbourne.multimap.btree.BTreeModule;
import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

import java.io.File;

public class RunModule extends AbstractModule {

    private String hdfsFile;
    private String indexFolder = "/tmp/index";

    public RunModule(String hdfsFile){
        this.hdfsFile = hdfsFile;
    }

    @Override
    protected void configure() {
        bind(Index.class).to(CSVIndex.class);

        bind(String.class).annotatedWith(Names.named("hdfsFile")).toInstance(hdfsFile);
        bind(File.class).annotatedWith(Names.named("indexFolder")).toInstance(new File(indexFolder));

        bind(Integer.class).annotatedWith(Names.named("csvColumn")).toInstance(1);

        install(new BTreeModule());
    }

    public void setIndexFolder(String s) {
        this.indexFolder = s;
    }
}
