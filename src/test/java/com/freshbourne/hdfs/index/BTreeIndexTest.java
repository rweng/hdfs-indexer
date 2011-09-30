package com.freshbourne.hdfs.index;

import com.freshbourne.hdfs.index.run.RunModule;
import com.freshbourne.util.FileUtils;

import com.google.inject.Guice;
import com.google.inject.Injector;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;


public class BTreeIndexTest {

    private CSVIndex index;
    private static File indexRootFolder = new File("/tmp/indexTest");
    private File indexFolder = new File(indexRootFolder + "/path/to/file.csv/");

    private static Injector injector;

    static {

        RunModule module = new RunModule("hdfs:///path/to/file.csv");
        module.setIndexFolder(indexRootFolder.getAbsolutePath());
        injector = Guice.createInjector(module);
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
        assertEquals( indexRootFolder + "/path/to/file.csv", index.getIndexDir().getAbsolutePath());
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

        index.close();
        openIndex();

        Iterator<String> i = index.getIterator();
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
