package com.freshbourne.hdfs.index.test;

import java.io.IOException;

import com.freshbourne.hdfs.index.ColumnIndex;
import com.freshbourne.hdfs.index.TreeMapIndex;
import com.freshbourne.hdfs.index.run.Col1Index;


import junit.framework.TestCase;

public class IndexTest extends TestCase {
	private TreeMapIndex index;
	
	public void setUp(){
		index = new ColumnIndex(1);
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
		String path = "/tmp/test_index"; 
		index.save(path);
		TreeMapIndex loadedIndex = TreeMapIndex.load(path);
		assertTrue(loadedIndex.containsKey("Blug"));
		assertEquals(index, loadedIndex);
		assertEquals(index.getHighestOffset(), loadedIndex.getHighestOffset());
	}
	
	public void testInheritance(){
		assertEquals(1, (new Col1Index()).getColumn());
	}
}
