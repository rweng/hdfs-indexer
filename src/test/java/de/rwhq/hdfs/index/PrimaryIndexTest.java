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

		public static class PrimaryIndexRecordReader extends IndexedRecordReaderTest {

			@Override
			protected Class<? extends AbstractIndexBuilder> getBuilderClass() {
				return null;
			}
		}
	}

	public static class PrimaryIndexedRecordReader extends IndexedRecordReaderTest {

		@Override
		protected Class<? extends AbstractIndexBuilder> getBuilderClass() {
			return CustomBuilder.class;
		}
	}

	public static class CustomBuilder extends AbstractIndexBuilder {

		@Test
		public void empty(){}

		@Override
		public BTreeIndexBuilder configure(BTreeIndexBuilder bTreeIndexBuilder) {
			return bTreeIndexBuilder
					.indexFolder(IndexedRecordReaderTest.INDEX)
					.addDefaultRange(new Range(1, 4))
					.cacheSize(10)
					.primaryIndex()
					.keySerializer(IntegerSerializer.INSTANCE)
					.keyExtractor(new IntegerCSVExtractor(0, ","))
					.comparator(IntegerComparator.INSTANCE);
		}
	}

}
