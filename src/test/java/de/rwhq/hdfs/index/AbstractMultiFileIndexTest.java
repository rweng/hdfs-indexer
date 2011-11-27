package de.rwhq.hdfs.index;

import de.rwhq.btree.Range;
import de.rwhq.comparator.IntegerComparator;
import de.rwhq.serializer.IntegerSerializer;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.SortedSet;

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractMultiFileIndexTest {
	private AbstractMultiFileIndex index;
	private KeyExtractor extractor       = new IntegerCSVExtractor(0, ",");
	private File         indexRootFolder = new File("/tmp/primaryIndexTest");
	private String       hdfsFilePath    = "/path/to/hdfs/file.csv";
	@Mock private FileSplit fileSplit;

	@Before
	public void setUp() throws IOException {
		MockitoAnnotations.initMocks(this);
		when(fileSplit.getStart()).thenReturn(0L);

		FileUtils.deleteDirectory(indexRootFolder);
		indexRootFolder.mkdir();
		index = (AbstractMultiFileIndex) setupBuilder().build();
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

		fillIndex(0, 10);
		index.sync();

		assertThat(index.isOpen()).isTrue();
		afterSyncTests();
	}

	@Test
	public void close() throws IOException {
		fillIndex(0, 10);
		index.close();

		assertThat(index.isOpen()).isFalse();
		afterSyncTests();
	}

	@Test
	public void maxPos() {
		fillIndex(0, 10);
		index.sync();

		assertThat(index.getMaxPos()).isEqualTo(99);
	}

	@Test
	public void toRanges() throws IOException {
		fillIndex(70, 40);
		index.sync();

		// this should not be returned when asking for ranges because it doesnt concern our split
		fillIndex(0, 10);
		index.sync();

		fillIndex(50, 10);
		index.close();

		index = (AbstractMultiFileIndex) setupBuilder()
				.fileSplit(fileSplit)
				.build();
		when(fileSplit.getStart()).thenReturn(300L);
		when(fileSplit.getLength()).thenReturn(1000L);

		index.open();
		SortedSet<Range<Long>> ranges = index.toRanges();

		assertThat(ranges).hasSize(2);
		assertThat(ranges.first()).isEqualTo(new Range(500L, 599L));
		assertThat(ranges.last()).isEqualTo(new Range(700L, 1099L));
	}

	@Test
	public void containsPos() throws IOException {
		fillIndex(50, 10);
		index.sync();

		fillIndex(70, 40);
		index.close();

		when(fileSplit.getStart()).thenReturn(500L);
		when(fileSplit.getLength()).thenReturn(100L);

		index = (AbstractMultiFileIndex) setupBuilder().build();
		index.open();

		assertThat(index.partialEndForPos(0L)).isEqualTo(-1);
		assertThat(index.partialEndForPos(500)).isEqualTo(599);
		assertThat(index.partialEndForPos(599)).isEqualTo(599);
		assertThat(index.partialEndForPos(600)).isEqualTo(-1);
		assertThat(index.partialEndForPos(733)).isEqualTo(1099);
	}

	@Test
	public void iteratorForRange() throws IOException {
		fillIndex(50, 10);
		index.sync();

		fillIndex(70, 40);
		index.close();

		when(fileSplit.getStart()).thenReturn(500L);
		when(fileSplit.getLength()).thenReturn(100L);

		index = (AbstractMultiFileIndex) setupBuilder().build();
		index.open();

		SortedSet<Range<Long>> sortedSet = index.toRanges();
		Range<Long> range = sortedSet.first();
		Iterator iterator = index.getIterator(range);

		for(int i = 50; i < 60;i++){
			assertThat(iterator.next()).isNotNull();
		}

		assertThat(iterator.next()).isNull();
	}

	@Test
	public void iteratorWithoutSearchRange() {
		int count = 10;
		fillIndex(0, count);
		when(fileSplit.getLength()).thenReturn(count * 10L);
		index.sync();

		// check iterator without default search range
		Iterator iterator = index.getIterator();
		for (int i = 0; i < count; i++) {
			assertThat(iterator.hasNext()).isTrue();
			assertThat(iterator.next()).isNotNull();
		}

		assertThat(iterator.hasNext()).isFalse();
		assertThat(iterator.next()).isNull();
	}

	@Test
	public void iteratorWithSearchRange() throws IOException {
		int count = 10;
		fillIndex(0, count);
		when(fileSplit.getLength()).thenReturn(count * 10L);
		index.close();

		index = (AbstractMultiFileIndex) setupBuilder()
				.addDefaultRange(new Range(1, 2))
				.addDefaultRange(new Range(1, 3))
				.addDefaultRange(new Range(9, 10))
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
	public void complyToRecordReaderSplitSize() throws IOException {
		fillIndex(0, 50);
		index.sync();

		fillIndex(50, 10);
		index.sync();

		fillIndex(60, 40);
		index.close();

		when(fileSplit.getStart()).thenReturn(500L);
		when(fileSplit.getLength()).thenReturn(100L);

		index = (AbstractMultiFileIndex) setupBuilder()
				.addDefaultRange(new Range(11, 12))
				.addDefaultRange(new Range(58, 59))
				.build();
		index.open();
		String matchString = "(58|59|60),.+";
		Iterator<String> iterator = index.getIterator();

		assertThat(iterator.next()).matches(matchString);
		assertThat(iterator.next()).matches(matchString);

		assertThat(iterator.next()).isEqualTo(null);
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
		assertThat(property.endPos).isEqualTo(99);
		assertThat(property.getFile()).isAbsolute().exists();

		// ensure that there is no lockfile
		assertThat(index.getLockFile()).doesNotExist();
		assertThat(index.isLocked()).isFalse();
	}

	/**
	 * @param from
	 * @param count
	 */
	private void fillIndex(int from, int count) {
		for (int i = from; i < from + count; i++) {
			String line = "" + i + ",name," + System.currentTimeMillis();
			line = line.substring(0,9);

			addToIndexInputStream(index, line + "\n", i * 10L);
			index.addLine(line, i * 10L, i * 10L + 9L);
		}
	}

	protected abstract void addToIndexInputStream(AbstractMultiFileIndex index, String line, long pos);

	private BTreeIndexBuilder setupBuilder() {
		return configureBuilder(new BTreeIndexBuilder()
				.indexRootFolder(indexRootFolder)
				.hdfsFilePath(hdfsFilePath)
				.comparator(IntegerComparator.INSTANCE)
				.fileSplit(fileSplit)
				.keyExtractor(extractor)
				.keySerializer(IntegerSerializer.INSTANCE));
	}

	protected abstract BTreeIndexBuilder configureBuilder(BTreeIndexBuilder b);
}
