package com.freshbourne.hdfs.index.test;

import java.io.IOException;

import com.freshbourne.hdfs.index.Index;


import junit.framework.TestCase;

public class IndexTest extends TestCase {
	private Index index;
	
	public void setUp(){
		index = new Index(0);
		String[] a = new String[1];
		a[0] = "Blug";
		index.add(a, 0);
		
	}
	
	public void testAdd(){
		String[] a = new String[1];
		a[0] = "Alfred";
		index.add(a, 5);
		assertTrue(index.containsKey("Alfred"));
	}
	
	public void testSave() throws IOException, ClassNotFoundException{
		String path = "/tmp/testindex"; 
		index.save(path);
		Index loadedIndex = Index.load(path);
		assertTrue(loadedIndex.containsKey("Blug"));
		assertEquals(index, loadedIndex);
		assertEquals(index.getHighestOffset(), loadedIndex.getHighestOffset());
	}
}
