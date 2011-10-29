package com.freshbourne.hdfs.index;

import com.freshbourne.btree.Range;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

public class BTreeIndexTest {

	private BTreeIndex<Integer> index;
	private static File indexRootFolder;
	private static File            indexFolder;

	private static String hdfsFile = "/path/to/file.csv";
	private static CSVModule module;
	private static final int CACHE_SIZE = 1000;
	private static final Log LOG = LogFactory.getLog(BTreeIndexTest.class);

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
		
		indexRootFolder = new File(module.indexRootFolder);
		indexFolder = new File(indexRootFolder + hdfsFile);
	}


	@BeforeMethod
	public void setUp() throws IOException {
		// index = factory.get()

		if (indexRootFolder.exists())
			FileUtils.deleteDirectory(indexRootFolder);
	}

	@Test
	public void empty(){
		
	}


	/*
	@Test
	public void creation() {
		assertTrue(index != null);
		assertEquals(hdfsFile, index.getHdfsFile());
		assertEquals(indexRootFolder + hdfsFile, index.getIndexDir().getAbsolutePath());
		assertEquals(indexRootFolder + "/path/to/file.csv/properties.xml", index.getPropertiesPath());
	}

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

	@Test
	public void indexFolderShouldNotBeAutomaticallyCreated() {
		assertFalse(indexFolder.exists());
	}

	@Test
	public void openShouldCreateFolders() throws Exception {
		index.open();
		assertTrue(index.isOpen());
		assertTrue(indexFolder.exists());
		assertTrue((new File(indexFolder.getAbsolutePath() + "/properties.xml")).exists());
	}

	@Test
	public void iteratorOnEmptyIndex() {
		openIndex();

		Iterator<String> i = index.getIterator();
		assertFalse(i.hasNext());
	}

	@Test
	public void addingStuffToIndex() throws IOException {
		openIndex();

		List<String> list = new LinkedList<String>();
		list.add("1    Robin  25");
		list.add("2    Fritz   55");

		assertFalse(index.getLockFile().exists());
		index.addLine(list.get(0), 0);
		assertTrue(index.getLockFile().exists());
		index.addLine(list.get(1), 10);
		index.close();
		index.open();
		assertEquals(10, index.getMaxPos());
		Iterator<String> i = index.getIterator(false);
		assertTrue(i.hasNext());
		assertTrue(list.contains(i.next()));
		assertTrue(list.contains(i.next()));
		assertFalse(i.hasNext());
		assertNull(i.next());


		// ensure lock if is deleted after close
		index.close();
		assertFalse(index.getLockFile().exists());

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
