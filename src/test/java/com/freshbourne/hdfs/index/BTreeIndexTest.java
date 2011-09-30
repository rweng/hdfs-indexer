package com.freshbourne.hdfs.index;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.AbstractMap;
import java.util.Iterator;

import static org.junit.Assert.*;


public class BTreeIndexTest {

    private BTreeIndex index;
    private File indexFolder = new File("/tmp/indexTest/path/to/file.csv/");

    private static Injector injector;

    static {
        injector =
                Guice.createInjector(new IndexModule("hdfs:///path/to/file.csv", new File("/tmp/indexTest"), "col1"));
    }

    @Before
    public void setUp() {
        index = injector.getInstance(BTreeIndex.class);
        if (indexFolder.exists())
            recursiveDelete(indexFolder);
    }

    private void recursiveDelete(File file) {
        if (file.isDirectory()) {
            for (File child : file.listFiles()) {
                recursiveDelete(child);
            }

        }
        file.delete();
    }

    @Test
    public void creation() {
        assertTrue(index != null);
        assertEquals("/path/to/file.csv", index.getHdfsFile());
        assertEquals("/tmp/indexTest/path/to/file.csv", index.getIndexDir().getAbsolutePath());
        assertEquals("/tmp/indexTest/path/to/file.csv/properties.xml", index.getPropertiesPath());
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

        Iterator<AbstractMap.SimpleEntry<String, String>> i = index.getIterator();
        assertFalse(i.hasNext());
    }

    @Test
    public void addingStuffToIndex() {
        openIndex();
        index.addLine("1,Robin,25", 0);
        index.addLine("2,Fritz,55", 10);

        index.close();
        openIndex();

        Iterator<AbstractMap.SimpleEntry<String, String>> i = index.getIterator();
        assertTrue(i.hasNext());
    }

    private void openIndex() {
        try {
            index.open();
        } catch (Exception e) {
            fail("index cannot be opened");
        }
    }
}
