package com.freshbourne.hdfs.index;

import com.freshbourne.util.FileUtils;

import com.google.inject.Guice;
import com.google.inject.Injector;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.*;

import static org.junit.Assert.*;


public class BTreeIndexTest {

    private CSVIndex index;
    private static File indexRootFolder;
    private static File indexFolder;

    private static Injector injector;

    private static void createInjector() {
	    RunModule module = new RunModule();
        injector = Guice.createInjector(module);

	    indexRootFolder  = new File(module.indexRootFolder());
	    indexFolder = new File(indexRootFolder + "/path/to/file.csv/");
    }

    static {
        createInjector();
    }

    @Before
    public void setUp() {
        index = injector.getInstance(CSVIndex.class);
        if (indexRootFolder.exists())
            FileUtils.recursiveDelete(indexRootFolder);
    }

    @Test
    public void creation() {
        assertTrue(index != null);
        assertEquals("/path/to/file.csv", index.getHdfsFile());
        assertEquals(indexRootFolder + "/path/to/file.csv", index.getIndexDir().getAbsolutePath());
        assertEquals(indexRootFolder + "/path/to/file.csv/properties.xml", index.getPropertiesPath());
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
        assertTrue((new File("/tmp/indexTest/path/to/file.csv/properties.xml")).exists());
    }

    @Test
    public void iteratorOnEmptyIndex() {
        openIndex();

        Iterator<String> i = index.getIterator();
        assertFalse(i.hasNext());
    }

    @Test
    public void addingStuffToIndex() {
        openIndex();

        List<String> list = new LinkedList<String>();
        list.add("1    Robin  25");
        list.add("2    Fritz   55");

        index.addLine(list.get(0), 0);
        index.addLine(list.get(1), 10);

        Iterator<String> i = index.getIterator();
        assertTrue(i.hasNext());
        assertTrue(list.contains(i.next()));
        assertTrue(list.contains(i.next()));
        assertFalse(i.hasNext());
        assertNull(i.next());
    }

    @Test
    public void secondIndex() throws Exception {
        addingStuffToIndex();
        List<String> list = new LinkedList<String>();
        list.add("1    Robin  25");
        list.add("2    Fritz   55");

        index.close();
        createInjector();
        CSVIndex index2 = injector.getInstance(CSVIndex.class);

        assertNotSame(index, index2);

        index2.open();
        Iterator<String> i = index2.getIterator();
        assertTrue(i.hasNext());
        assertTrue(list.contains(i.next()));
        assertTrue(list.contains(i.next()));
        assertFalse(i.hasNext());
        assertNull(i.next());
    }

    private void openIndex() {
        try {
            index.open();
        } catch (Exception e) {
            fail("index cannot be opened");
        }
    }
}
