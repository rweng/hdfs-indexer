package de.rwhq.hdfs.index;

import de.rwhq.btree.Range;
import de.rwhq.comparator.IntegerComparator;
import de.rwhq.serializer.IntegerSerializer;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

@RunWith(Enclosed.class)
public class PrimaryIndexTest {

	public static class Main extends AbstractMultiFileIndexTest {
		@Override
		protected void addToIndexInputStream(AbstractMultiFileIndex index, String line, long pos) {
			// not required here
		}

		@Override
		protected BTreeIndexBuilder configureBuilder(BTreeIndexBuilder b) {
			return b.primaryIndex();
		}

	}

	public static class PrimaryIndexedRecordReader extends IndexedRecordReaderTest {

		@Override
		protected Class<? extends SpyBuilder> getBuilderClass() {
			return CustomBuilder.class;
		}
	}

	public static class CustomBuilder extends IndexedRecordReaderTest.SpyBuilder {

		@Test
		public void empty(){}

		@Override
		protected BTreeIndexBuilder configure2(BTreeIndexBuilder b) {
			return b.primaryIndex();
		}
	}

}
