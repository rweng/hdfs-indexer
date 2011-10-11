package com.freshbourne.hdfs.index;

import com.freshbourne.btree.BTreeFactory;
import com.freshbourne.btree.Range;
import com.freshbourne.serializer.FixLengthSerializer;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.sun.istack.internal.Nullable;

import java.io.File;
import java.util.Comparator;
import java.util.List;

@Singleton
public class BTreeIndexBuilder<K> {
	@Inject @Named("hdfsFile")    String                         hdfsFile;
	@Inject @Named("indexFolder") File                           indexFolder;
	@Inject @Named("indexId")     String                         indexId;
	@Inject                       BTreeFactory                   factory;
	@Inject                       FixLengthSerializer<K, byte[]> keySerializer;
	@Inject                       Comparator<K>                  comparater;
	@Inject @Nullable             List<Range<K>>                 defaultSearchRanges;
}
