package de.rwhq.hdfs.index;

import com.google.common.collect.Collections2;
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
import java.util.*;

import static org.fest.assertions.Assertions.assertThat;

public class PrimaryIndexTest {
	private PrimaryIndex<Integer> index;
	protected final File   indexRootFolder = new File("/tmp/BTreeIndexTest");
	protected final String hdfsFile        = "/path/to/file.csv";

	@BeforeMethod
	public void setUp() throws IOException {
		if (indexRootFolder.exists())
			FileUtils.deleteDirectory(indexRootFolder);
		indexRootFolder.mkdir();

		index = (PrimaryIndex<Integer>) setUpBuilder().build();
		index.open();
	}

	@Test
	public void maxPos() throws IOException {
		IndexTest.fillIndex(index, 10);
		index.sync();
		
		assertThat(index.getMaxPos()).isEqualTo(90L);
	}

	@Test
	public void secondIndex() throws Exception {
		//addEntriesToIndex();
		index.close();

		PrimaryIndex index2 = (PrimaryIndex) setUpBuilder().build();
		assertThat(index).isNotSameAs(index2);

		index2.open();
		Iterator<String> i = index2.getIterator(false);
		assertThat(i.hasNext()).isTrue();
		assertThat(i.next()).isNotNull();
		assertThat(i.next()).isNotNull();
		assertThat(i.next()).isNotNull();
		assertThat(i.hasNext()).isFalse();
		assertThat(i.next()).isNull();
	}

	@Test(dependsOnMethods = "secondIndex")
	public void testRange() throws IOException {
		index.open();

		IndexTest.fillIndex(index, 100);

		index.close();

		ArrayList<Range<Integer>> ranges = Lists.newArrayList();
		ranges.add(new Range(0, 10));
		ranges.add(new Range(50, 55));
		ranges.add(new Range(99, null));

		index = (PrimaryIndex<Integer>) setUpBuilder().defaultSearchRanges(ranges).build();
		index.open();

		Iterator<String> iterator = index.getIterator();

		for (int i = 0; i <= 10; i++) {
			assertThat(iterator.next()).startsWith("" + i);
		}


		for (int i = 50; i <= 55; i++)
			assertThat(iterator.next()).startsWith("" + i);

		assertThat(iterator.next()).startsWith("99");
		assertThat(iterator.hasNext()).isFalse();
	}

	@Test
	public void indexFolderPath() {
		assertThat(getIndex().getIndexFolder().getAbsolutePath()).isEqualTo(
				indexRootFolder.getAbsolutePath() + hdfsFile);
	}

	@Factory
	public Object[] interfaces() {
		return new Object[]{
				new IndexTest() {
					@Override
					protected Index getNewIndex() {
						return setUpBuilder().build();

					}

					@Override
					protected Index resetIndex() throws IOException {
						PrimaryIndexTest.this.setUp();
						return index;
					}
				},
				new AbstractMultiFileIndexTest() {
					@Override
					protected AbstractMultiFileIndex resetIndex() throws IOException {
						PrimaryIndexTest.this.setUp();
						return index;
					}

					@Override
					protected AbstractMultiFileIndex getNewIndex() {
						return (SecondaryIndex) setUpBuilder().build();
					}
				}};
	}

	protected AbstractMultiFileIndex getIndex() {
		return index;
	}

	protected BTreeIndexBuilder setUpBuilder() {
		return new BTreeIndexBuilder()
				.primaryIndex()
				.indexFolder(indexRootFolder)
				.hdfsFilePath(hdfsFile)
				.keyExtractor(new IntegerCSVExtractor(0, ","))
				.keySerializer(IntegerSerializer.INSTANCE)
				.comparator(IntegerComparator.INSTANCE)
				.addDefaultRange(new Range<Integer>(0, 10))
				.addDefaultRange(new Range<Integer>(-5, 5))
				.addDefaultRange(new Range<Integer>(50, 55))
				.addDefaultRange(new Range<Integer>(99, 99))
				.addDefaultRange(new Range<Integer>(100, 1010));
	}
}
