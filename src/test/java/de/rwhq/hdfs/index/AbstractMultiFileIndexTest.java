package de.rwhq.hdfs.index;

import de.rwhq.btree.Range;
import de.rwhq.comparator.IntegerComparator;
import de.rwhq.hdfs.index.extractor.IntegerCSVExtractor;
import de.rwhq.hdfs.index.extractor.KeyExtractor;
import de.rwhq.serializer.IntegerSerializer;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.SortedSet;

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.when;

public abstract class AbstractMultiFileIndexTest {
	private AbstractMultiFileIndex<Long, Long> index;
	private KeyExtractor extractor       = new IntegerCSVExtractor(0, ",");
	private File         indexRootFolder = new File("/tmp/primaryIndexTest");
	private String       hdfsFilePath    = "/path/to/hdfs/file.csv";
	@Mock private FileSplit fileSplit;

	@Before
	public void setUp() throws IOException {
		MockitoAnnotations.initMocks(this);
		when(fileSplit.getStart()).thenReturn(0L);
		when(fileSplit.getLength()).thenReturn(1000000L);
		when(fileSplit.getPath()).thenReturn(new Path(hdfsFilePath));

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

	@Test @Ignore("I dont know why this isn't working")
	public void containsPos() throws IOException {
		fillIndex(50, 10);
		index.sync();

		fillIndex(70, 40);
		index.close();

		when(fileSplit.getStart()).thenReturn(500L);
		when(fileSplit.getLength()).thenReturn(100L);

		index = (AbstractMultiFileIndex) setupBuilder().build();
		index.open();

		assertThat(getRangeForPos(0L)).isEqualTo(-1);
		assertThat(getRangeForPos(500)).isEqualTo(599);
		assertThat(getRangeForPos(599)).isEqualTo(599);
		assertThat(getRangeForPos(600)).isEqualTo(-1);
		assertThat(getRangeForPos(733)).isEqualTo(1099);
	}

	private Range<Long> getRangeForPos(long pos) {

		for (Range<Long> p : index.toRanges()) {
			if (p.getFrom().compareTo(pos) <= 0 && p.getTo().compareTo(pos) >= 0) {
				return p;
			}
		}

		return null;
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

		for (int i = 50; i < 60; i++) {
			assertThat(iterator.next()).isNotNull();
		}

		assertThat(iterator.hasNext()).isFalse();
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

		assertThat(iterator.hasNext()).isFalse();
	}

	@Test
	public void continuousFill() throws IOException {
		fillIndex(0, 50);
		index.close();

		index = (AbstractMultiFileIndex) setupBuilder().build();
		index.open();
		fillIndex(50, 10);

		index.sync();

		assertThat(index.toRanges()).hasSize(2).contains(new Range(0L, 499L), new Range(500L, 599L));
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

		assertThat(iterator.hasNext()).isFalse();
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
			line = line.substring(0, 9);

			addToIndexInputStream(index, line + "\n", i * 10L);
			index.addLine(line, i * 10L, i * 10L + 9L);
		}
	}

	protected abstract void addToIndexInputStream(AbstractMultiFileIndex index, String line, long pos);

	private IndexBuilder setupBuilder() {
		return configureBuilder(new IndexBuilder()
				.indexRootFolder(indexRootFolder)
				.comparator(IntegerComparator.INSTANCE)
				.fileSplit(fileSplit)
				.keyExtractor(extractor)
				.treePageSize(4 * 1024)
				.maxPartialsPerSplit(100)
				.keySerializer(IntegerSerializer.INSTANCE));
	}

	protected abstract IndexBuilder configureBuilder(IndexBuilder b);
}
