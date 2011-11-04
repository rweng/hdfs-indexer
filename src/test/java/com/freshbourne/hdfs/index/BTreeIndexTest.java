package com.freshbourne.hdfs.index;

import com.freshbourne.btree.Range;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.annotations.BeforeGroups;
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
	private static CSVModule module;
	private static final int CACHE_SIZE = 1000;
	private static final Log LOG        = LogFactory.getLog(BTreeIndexTest.class);

	private static void createInjector() {
		module = new CSVModule();
		module.cacheSize = CACHE_SIZE;

		module.hdfsFile = hdfsFile;

		module.searchRange.add(new Range<Integer>(0, 10));
		module.searchRange.add(new Range<Integer>(-5, 5));
		module.searchRange.add(new Range<Integer>(0, 10));
		module.searchRange.add(new Range<Integer>(50, 55));
		module.searchRange.add(new Range<Integer>(99, 99));
		module.searchRange.add(new Range<Integer>(100, 1010));

	}

	@BeforeMethod
	public void build() throws IOException {
		if (indexRootFolder.exists())
			FileUtils.deleteDirectory(indexRootFolder);
		indexRootFolder.mkdir();

		index = new BTreeIndexBuilder().cacheSize(1000).indexFolder(indexRootFolder).hdfsFilePath(hdfsFile).build();
		assertThat(index).isNotNull();
	}

	@Test(dependsOnMethods = "open")
	public void indexFolder() {
		assertThat(index.getIndexFolder().getAbsolutePath()).isEqualTo(indexRootFolder.getAbsolutePath() + hdfsFile);
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
	public void iteratorOnEmptyIndex() {
		Iterator<String> i = index.getIterator();
		assertThat(i.hasNext()).isFalse();
	}


	@Test //(dependsOnMethods = "open")
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

		/*
		  Iterator<String> i = index.getIterator(false);
		  assertTrue(i.hasNext());
		  assertTrue(list.contains(i.next()));
		  assertTrue(list.contains(i.next()));
		  assertFalse(i.hasNext());
		  assertNull(i.next());


		  // ensure lock if is deleted after close
		  index.close();
		  assertFalse(index.getLockFile().exists());
  */
	}

/*



	/*

	@Test
	public void cache() throws IOException {
		index.open();
		fillIndex(index, CACHE_SIZE + 1);
		index.close();

		String[] files = new File(indexRootFolder + hdfsFile).list();
		assertEquals(3, files.length);
		index.open();
		Iterator<String> iterator = index.getIterator(false);
		for (int i = 0; i < (CACHE_SIZE + 1); i++) {
			assertNotNull(iterator.next());
		}
		assertNull(iterator.next());
	}

	/**
	@Test
	public void secondIndex() throws Exception {
		addingStuffToIndex();
		List<String> list = new LinkedList<String>();
		list.add("1    Robin  25");
		list.add("2    Fritz   55");

		index.close();
		createInjector();
		StringCSVIndex index2 = injector.getInstance(StringCSVIndex.class);

		assertNotSame(index, index2);

		index2.open();
		Iterator<String> i = index2.getIterator(false);
		assertTrue(i.hasNext());
		assertTrue(list.contains(i.next()));
		assertTrue(list.contains(i.next()));
		assertFalse(i.hasNext());
		assertNull(i.next());
	}

	@Test
	public void maxPos() throws IOException {
		addingStuffToIndex();
		index.open();
		assertEquals(10, index.getMaxPos());
	}

	@Test
	public void testRange() throws IOException {
		intIndex.open();

		fillIndex(intIndex, 100);

		intIndex.close();
		intIndex.open();

		List<Range<Integer>> ranges = new ArrayList<Range<Integer>>();
		ranges.add(new Range<Integer>(-5, 5));
		ranges.add(new Range<Integer>(0, 10));
		ranges.add(new Range<Integer>(50, 55));
		ranges.add(new Range<Integer>(99, 99));
		ranges.add(new Range<Integer>(100, 1010));

		Iterator<String> iterator = intIndex.getIterator(ranges);

		for (int i = 0; i <= 10; i++) {
			assertEquals("" + i + " col2", iterator.next());
		}


		for (int i = 50; i <= 55; i++)
			assertEquals("" + i + " col2", iterator.next());

		assertEquals("99 col2", iterator.next());
		assertFalse(iterator.hasNext());
	}


	@Test
	public void testDefaultRanges() throws IOException {
		intIndex.open();

		for (int i = 0; i < 100; i++) {
			intIndex.addLine("" + i + " col2", i);
		}

		intIndex.close();
		intIndex.open();

		Iterator<String> iterator = intIndex.getIterator();

		for (int i = 0; i <= 10; i++) {
			assertEquals("" + i + " col2", iterator.next());
		}


		for (int i = 50; i <= 55; i++)
			assertEquals("" + i + " col2", iterator.next());

		assertEquals("99 col2", iterator.next());
		assertFalse(iterator.hasNext());
	}
	**/

/*
	private void fillIndex(Index index, int count) {
		for (int i = 0; i < count; i++) {
			index.addLine("" + i + " col2", i);
		}
	}


	private void openIndex() {
		try {
			// index.open();
		} catch (Exception e) {
			fail("index cannot be opened");
		}
	}
*/
}
