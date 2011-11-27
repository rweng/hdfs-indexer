package de.rwhq.hdfs.index;

public class PrimaryIndexTest extends AbstractMultiFileIndexTest {
	@Override
	protected void addToIndexInputStream(AbstractMultiFileIndex index, String line, long pos) {
		// not required here
	}

	@Override
	protected BTreeIndexBuilder configureBuilder(BTreeIndexBuilder b) {
		return b.primaryIndex();
	}
}
