package de.rwhq.hdfs.index;

import de.rwhq.comparator.IntegerComparator;
import de.rwhq.serializer.IntegerSerializer;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

import static org.fest.assertions.Assertions.assertThat;

public class SecondaryIndexTest {

	private SecondaryIndex index;
	private final File indexFolder = new File("/tmp/secondaryTest");

	@BeforeMethod
	public void setUp() throws IOException {
		FileUtils.deleteDirectory(indexFolder);
		indexFolder.mkdir();

		index = (SecondaryIndex) setUpBuilder().build();
		index.open();
	}

	@Test
	public void creation(){
		assertThat(index.isOpen()).isTrue();
	}

	@Factory
	public Object[] createInterfaceTests(){
		return new Object[]{new AbstractMultiFileIndexTest(){
			@Override
			protected AbstractMultiFileIndex getNewIndex() {
				return (AbstractMultiFileIndex) setUpBuilder().build();
			}
		}};
	}



	private BTreeIndexBuilder setUpBuilder(){
		return new BTreeIndexBuilder()
				.hdfsFilePath("/test/secondaryIndexFile")
				.indexFolder(indexFolder)
				.keyExtractor(new IntegerCSVExtractor(0, ","))
				.keySerializer(IntegerSerializer.INSTANCE)
				.comparator(IntegerComparator.INSTANCE);
	}
}
