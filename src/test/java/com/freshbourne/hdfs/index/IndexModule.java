package com.freshbourne.hdfs.index;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

import java.io.File;

public class IndexModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(String.class).annotatedWith(Names.named("hdfsFile")).toInstance("hdfs:///path/to/file.csv");
        bind(File.class).annotatedWith(Names.named("indexFolder")).toInstance(new File("/tmp/indexTest"));
        bind(String.class).annotatedWith(Names.named("indexId")).toInstance("col1");
    }
}
