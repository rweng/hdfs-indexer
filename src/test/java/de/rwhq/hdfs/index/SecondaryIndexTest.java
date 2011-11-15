package de.rwhq.hdfs.index;

import org.testng.annotations.BeforeMethod;

public class SecondaryIndexTest extends AbstractMultiFileIndexTest {
	@BeforeMethod
	public void setUp() throws Exception {
		
	}

	@Override
	protected AbstractMultiFileIndex getIndex() {
		return null;
	}
}
