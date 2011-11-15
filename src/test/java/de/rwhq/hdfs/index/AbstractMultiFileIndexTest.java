package de.rwhq.hdfs.index;

import de.rwhq.btree.Range;
import de.rwhq.comparator.IntegerComparator;
import de.rwhq.serializer.IntegerSerializer;
import org.apache.commons.io.FileUtils;
import org.fest.assertions.Assertions;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

import static org.fest.assertions.Assertions.assertThat;

public abstract class AbstractMultiFileIndexTest {
	protected final File   indexRootFolder = new File("/tmp/BTreeIndexTest");
	protected final String hdfsFile        = "/path/to/file.csv";


	@Test
	public void open() throws IOException {
		Assertions.assertThat(getIndex().getIndexFolder()).doesNotExist();

		getIndex().open();
		Assertions.assertThat(getIndex().isOpen()).isTrue();
		Assertions.assertThat(getIndex().getIndexFolder()).exists();

		File file = new File(getIndex().getIndexFolder().getAbsolutePath() + "/properties.xml");
		assertThat(file).exists();
	}

	protected BTreeIndexBuilder setUpBuilder() {
		return new BTreeIndexBuilder()
				.indexFolder(indexRootFolder)
				.hdfsFilePath(hdfsFile)
				.keyExtractor(new IntegerCSVExtractor(0, "( |\\t)+"))
				.keySerializer(IntegerSerializer.INSTANCE)
				.comparator(IntegerComparator.INSTANCE)
				.addDefaultRange(new Range<Integer>(0, 10))
				.addDefaultRange(new Range<Integer>(-5, 5))
				.addDefaultRange(new Range<Integer>(50, 55))
				.addDefaultRange(new Range<Integer>(99, 99))
				.addDefaultRange(new Range<Integer>(100, 1010));
	}

@Test(dependsOnMethods = "open")
	public void indexFolder() {
		assertThat(getIndex().getIndexFolder().getAbsolutePath()).isEqualTo(indexRootFolder.getAbsolutePath() + hdfsFile);
	}


	protected abstract AbstractMultiFileIndex getIndex();
}
