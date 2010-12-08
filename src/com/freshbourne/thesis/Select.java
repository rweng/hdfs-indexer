package com.freshbourne.thesis;

public abstract class Select {
	private int column;
	
	public Select(int column){
		this.column = column;
	}
	
	public boolean select(String[] a){
		if( a.length < column )
			return false;
		
		return select(a[column]);
	}
	
	public abstract boolean select(String s);
	
}
