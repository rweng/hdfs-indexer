package de.rwhq.hdfs.index;

import com.google.common.collect.Lists;
import de.rwhq.btree.Range;
import de.rwhq.comparator.IntegerComparator;
import de.rwhq.serializer.IntegerSerializer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static org.fest.assertions.Assertions.assertThat;

public class BTreeIndexTest {

	private BTreeIndex<Integer> index;
	private static final File   indexRootFolder = new File("/tmp/BTreeIndexTest");
	private static       String hdfsFile        = "/path/to/file.csv";
	private static final int    CACHE_SIZE      = 1000;
	private static final Log    LOG             = LogFactory.getLog(BTreeIndexTest.class);

	/*
	private static void createInjector() {
		module = new CSVModule();
		module.cacheSize = CACHE_SIZE;

		module.hdfsFile = hdfsFile;


	}
	*/

	@BeforeMethod
	public void build() throws IOException {
		if (indexRootFolder.exists())
			FileUtils.deleteDirectory(indexRootFolder);
		indexRootFolder.mkdir();

		index = setUpBuilder().build();
		assertThat(index).isNotNull();
	}

	@Test(dependsOnMethods = "open")
	public void indexFolder() {
		assertThat(index.getIndexFolder().getAbsolutePath()).isEqualTo(indexRootFolder.getAbsolutePath() + hdfsFile);
	}

	private BTreeIndexBuilder setUpBuilder() {
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


	@Test
	public void open() throws IOException {
		assertThat(index.getIndexFolder()).doesNotExist();

		index.open();
		assertThat(index.isOpen()).isTrue();
		assertThat(index.getIndexFolder()).exists();

		File file = new File(index.getIndexFolder().getAbsolutePath() + "/properties.xml");
		assertThat(file).exists();
	}

	@Test(dependsOnMethods = "open")
	public void iteratorOnEmptyIndex() throws IOException {
		open();

		Iterator<String> i = index.getIterator();
		assertThat(i.hasNext()).isFalse();
	}


	@Test(dependsOnMethods = "open")
	public void addingStuffToIndex() throws IOException {
		open();

		List<String> list = Lists.newArrayList();
		list.add("1    Robin  25");
		list.add("2    Fritz   55");

		index.addLine(list.get(0), 0);
		index.addLine(list.get(1), 10);
		index.close();
		index.open();

		assertThat(index.getMaxPos()).isEqualTo(10);
		Iterator<String> i = index.getIterator(false);
		assertThat(i.hasNext()).isTrue();
		assertThat(list.contains(i.next())).isTrue();
		assertThat(list.contains(i.next())).isTrue();
		assertThat(i.hasNext()).isFalse();
		assertThat(i.next()).isNull();

		// ensure lock if is deleted after close
		index.close();
		assertThat(index.getLockFile()).doesNotExist();
	}

	@Test
	public void maxPos() throws IOException {
		addingStuffToIndex();
		index.open();
		assertThat(index.getMaxPos()).isEqualTo(10);
	}

	@Test(dependsOnMethods = "addingStuffToIndex")
	public void secondIndex() throws Exception {
		addingStuffToIndex();
		List<String> list = Lists.newArrayList();
		list.add("1    Robin  25");
		list.add("2    Fritz   55");
		index.close();

		BTreeIndex index2 = setUpBuilder().build();
		assertThat(index).isNotSameAs(index2);

		index2.open();
		Iterator<String> i = index2.getIterator(false);
		assertThat(i.hasNext()).isTrue();
		assertThat(list.contains(i.next())).isTrue();
		assertThat(list.contains(i.next())).isTrue();
		assertThat(i.hasNext()).isFalse();
		assertThat(i.next()).isNull();
	}

	@Test(dependsOnMethods = "secondIndex")
	public void testRange() throws IOException {
		index.open();

		fillIndex(index, 100);

		index.close();
		index.open();

		List<Range<Integer>> ranges = Lists.newArrayList();
		ranges.add(new Range<Integer>(-5, 5));
		ranges.add(new Range<Integer>(0, 10));
		ranges.add(new Range<Integer>(50, 55));
		ranges.add(new Range<Integer>(99, 99));
		ranges.add(new Range<Integer>(100, 1010));

		Iterator<String> iterator = index.getIterator(ranges);

		for (int i = 0; i <= 10; i++) {
			assertThat(iterator.next()).isEqualTo("" + i + " col2");
		}


		for (int i = 50; i <= 55; i++)
			assertThat(iterator.next()).isEqualTo("" + i + " col2");
		
		assertThat(iterator.next()).isEqualTo("99 col2");
		assertThat(iterator.hasNext()).isFalse();
	}


	@Test(dependsOnMethods = "testRange")
	public void testDefaultRanges() throws IOException {
		index.open();

		for (int i = 0; i < 100; i++) {
			index.addLine("" + i + " col2", i);
		}

		index.close();
		index.open();

		Iterator<String> iterator = index.getIterator();

		for (int i = 0; i <= 10; i++) {
			assertThat(iterator.next()).isEqualTo("" + i + " col2");
		}


		for (int i = 50; i <= 55; i++)
			assertThat(iterator.next()).isEqualTo("" + i + " col2");

		assertThat(iterator.next()).isEqualTo("99 col2");
		assertThat(iterator.hasNext()).isFalse();
	}

	private void fillIndex(Index index, int count) {
		for (int i = 0; i < count; i++) {
			index.addLine("" + i + " col2", i);
		}
	}
}