/**
 * Copyright (C) 2011 Robin Wenglewski <robin@wenglewski.de>
 *
 * This work is licensed under a Creative Commons Attribution-NonCommercial 3.0 Unported License:
 * http://creativecommons.org/licenses/by-nc/3.0/
 * For alternative conditions contact the author. 
 */
package com.freshbourne.hdfs.index.run;

import java.io.Serializable;

/**
 * This very basic kind of a filter supports only finding exact matches, although the BTree already supports range
 * queries.
 * 
 * @author Robin Wenglewski <robin@wenglewski.de>
 *
 */
public class Filter implements Serializable {

	private static final long serialVersionUID = 1L;
	
	public String getFilter(){
		return "2006-03-17";
	}
	
}
