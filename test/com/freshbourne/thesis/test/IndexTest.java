package com.freshbourne.thesis.test;

import com.freshbourne.thesis.Index;

import junit.framework.TestCase;

public class IndexTest extends TestCase {
	private Index index;
	
	public void setUp(){
		index = new Index(0);
	}
	
	public void testAdd(){
		String[] a = new String[1];
		a[0] = "Alfred";
		index.add(a, 0);
		assertTrue(index.containsKey("Alfred"));
	}
}
