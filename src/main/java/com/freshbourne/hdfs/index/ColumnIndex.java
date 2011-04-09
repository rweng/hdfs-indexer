package com.freshbourne.hdfs.index;

public class ColumnIndex extends TreeMapIndex {

	private static final long serialVersionUID = 1L;
	
	private int column;
	public int getColumn(){return column;}
	public void setColumn(int c){column = c;}
	
	public ColumnIndex(int c){
		super();
		column = c;
	}
	
	public ColumnIndex(){super();}
	
	@Override
	public void add(String[] splits, Long offset){
		if(offset <= highestOffset)
			return;
		
		if(splits.length > column){
			highestOffset = offset;
			put(splits[column], offset);
		}
		LOG.info(toString());
	}
}
