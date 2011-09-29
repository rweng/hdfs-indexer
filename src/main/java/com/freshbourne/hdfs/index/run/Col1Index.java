package com.freshbourne.hdfs.index.run;

import java.io.File;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.inject.name.Named;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.freshbourne.hdfs.index.CSVIndex;
import com.freshbourne.multimap.btree.BTree;
import com.freshbourne.multimap.btree.BTreeModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import org.apache.log4j.Logger;

public class Col1Index extends CSVIndex {

	protected Col1Index(@Named("hdfsFile") String hdfsFile, @Named("indexFolder") File indexFolder, @Named("indexId") String indexId) {
        super(hdfsFile, indexFolder, "2");
    }
}
