package de.rwhq.hdfs.index;

import com.google.common.collect.Lists;
import de.rwhq.btree.Range;
import de.rwhq.comparator.IntegerComparator;
import de.rwhq.serializer.IntegerSerializer;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static org.fest.assertions.Assertions.assertThat;

public class PrimaryIndexTest {
	/*

	private PrimaryIndex<Integer> index;
	protected final File   indexRootFolder = new File("/tmp/BTreeIndexTest");
	protected final String hdfsFile        = "/path/to/file.csv";

	@BeforeMethod
	public void build() throws IOException {
		if (indexRootFolder.exists())
			FileUtils.deleteDirectory(indexRootFolder);
		indexRootFolder.mkdir();

		index = (PrimaryIndex<Integer>) setUpBuilder().build();
		assertThat(index).isNotNull();
	}

	@Test(dependsOnMethods = "add2EntriesToIndex")
	public void maxPos() throws IOException {
		add2EntriesToIndex();
		index.open();
		assertThat(index.getMaxPos()).isEqualTo(10);
	}

	@Test(dependsOnMethods = "add2EntriesToIndex")
	public void secondIndex() throws Exception {
		add2EntriesToIndex();
		List<String> list = Lists.newArrayList();
		list.add("1    Robin  25");
		list.add("2    Fritz   55");
		index.close();

		PrimaryIndex index2 = (PrimaryIndex) setUpBuilder().build();
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

	protected AbstractMultiFileIndex getIndex() {
		return index;
	}

	@Test
	public void open() throws IOException {
		assertThat(getIndex().getIndexFolder()).doesNotExist();

		getIndex().open();
		assertThat(getIndex().isOpen()).isTrue();
		assertThat(getIndex().getIndexFolder()).exists();

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

		*/
}
