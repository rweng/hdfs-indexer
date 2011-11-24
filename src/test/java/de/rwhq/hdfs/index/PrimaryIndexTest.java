package de.rwhq.hdfs.index;

import de.rwhq.btree.Range;
import de.rwhq.comparator.IntegerComparator;
import de.rwhq.serializer.IntegerSerializer;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import static org.fest.assertions.Assertions.assertThat;

public class PrimaryIndexTest {
	private PrimaryIndex index;
	private KeyExtractor extractor       = new IntegerCSVExtractor(0, ",");
	private File         indexRootFolder = new File("/tmp/primaryIndexTest");
	private String       hdfsFilePath    = "/path/to/hdfs/file.csv";

	@Before
	public void setUp() throws IOException {
		FileUtils.deleteDirectory(indexRootFolder);
		indexRootFolder.mkdir();
		index = (PrimaryIndex) setupBuilder().build();
		index.open();
	}

	@Test
	public void indexFolder() {
		assertThat(index.getIndexFolder().getAbsolutePath()).isEqualTo(
				indexRootFolder.getAbsolutePath() + hdfsFilePath);
	}

	@Test
	public void sync() throws IOException {
		assertThat(index.getLockFile()).doesNotExist();

		fillIndex(10);
		index.sync();

		assertThat(index.isOpen()).isTrue();
		afterSyncTests();
	}

	@Test
	public void close() throws IOException {
		fillIndex(10);
		index.close();

		assertThat(index.isOpen()).isFalse();
		afterSyncTests();
	}

	@Test
	public void maxPos() {
		fillIndex(10);
		index.sync();

		assertThat(index.getMaxPos()).isEqualTo(90);
	}

	private void afterSyncTests() throws IOException {
		// ensure folder is created
		assertThat(index.getIndexFolder()).exists();

		// ensure properties file is created
		File propertiesFile = new File(index.getIndexFolder().getAbsolutePath() + "/properties");
		assertThat(propertiesFile).exists();

		// ensure properties can be read
		MFIProperties properties = new MFIProperties(propertiesFile.getAbsolutePath());
		properties.read();
		assertThat(properties.asList().size()).isEqualTo(1);

		// ensure properties are correct and tree exists
		MFIProperties.MFIProperty property = properties.asList().get(0);
		assertThat(property.startPos).isEqualTo(0L);
		assertThat(property.endPos).isEqualTo(90);
		assertThat(property.getFile()).isAbsolute().exists();

		// ensure that there is no lockfile
		assertThat(index.getLockFile()).doesNotExist();
		assertThat(index.isLocked()).isFalse();
	}

	@Test
	public void iteratorWithoutSearchRange(){
		int count = 10;
		fillIndex(count);
		index.sync();

		// check iterator without default search range
		Iterator iterator = index.getIterator();
		for(int i = 0; i<count;i++){
			assertThat(iterator.hasNext()).isTrue();
			assertThat(iterator.next()).isNotNull();
		}

		assertThat(iterator.hasNext()).isFalse();
		assertThat(iterator.next()).isNull();
	}

	@Test
	public void iteratorWithSearchRange() throws IOException {
		int count = 10;
		fillIndex(count);
		index.close();

		index = (PrimaryIndex) setupBuilder()
				.addDefaultRange(new Range(1,2))
				.addDefaultRange(new Range(1,3))
				.addDefaultRange(new Range(9,10))
				.build();
		index.open();
		String matchString = "(1|2|3|9),.+";
		Iterator<String> iterator = index.getIterator();

		assertThat(iterator.next()).matches(matchString);
		assertThat(iterator.next()).matches(matchString);
		assertThat(iterator.next()).matches(matchString);
		assertThat(iterator.next()).matches(matchString);

		assertThat(iterator.next()).isEqualTo(null);
		
	}

	@Test
	@Ignore
	public void iteratorShouldOnlyIterateToSplitEnd(){}

	private void fillIndex(int count) {
		for (int i = 0; i < count; i++) {
			index.addLine("" + i + ",name," + System.currentTimeMillis(), i * 10L);
		}
	}

	private BTreeIndexBuilder setupBuilder() {
		return new BTreeIndexBuilder()
				.primaryIndex()
				.indexRootFolder(indexRootFolder)
				.hdfsFilePath(hdfsFilePath)
				.comparator(IntegerComparator.INSTANCE)
				.keyExtractor(extractor)
				.keySerializer(IntegerSerializer.INSTANCE);
	}


}
