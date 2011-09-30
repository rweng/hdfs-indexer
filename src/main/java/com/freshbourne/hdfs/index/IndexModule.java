package com.freshbourne.hdfs.index;

import com.freshbourne.multimap.btree.BTreeModule;
import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

import java.io.File;
import java.io.Serializable;

public class IndexModule extends AbstractModule implements Serializable {
    

    private String hdfsFile;
    private File indexFolder;
    private String indexId;

    public IndexModule(String hdfsFile, File indexFolder, String indexId) {
        this.hdfsFile = hdfsFile;
        this.indexFolder = indexFolder;
        this.indexId = indexId;
    }

    public IndexModule(String hdfsFile, String indexFolder, String indexId) {
        this.hdfsFile = hdfsFile;
        this.indexFolder = new File(indexFolder);
        this.indexId = indexId;
    }


    @Override
    protected void configure() {
        bind(String.class).annotatedWith(Names.named("hdfsFile")).toInstance(hdfsFile);
        bind(File.class).annotatedWith(Names.named("indexFolder")).toInstance(indexFolder);
        bind(String.class).annotatedWith(Names.named("indexId")).toInstance(indexId);
        
        install(new BTreeModule());
    }
}
