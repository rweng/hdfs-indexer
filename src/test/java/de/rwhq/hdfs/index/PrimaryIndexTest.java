package de.rwhq.hdfs.index;

import de.rwhq.serializer.StringCutSerializer;
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
		protected IndexBuilder configureBuilder(IndexBuilder b) {
			return b.primaryIndex().valueSerializer(StringCutSerializer.get(500));
		}

	}

	public static class IndexedRecordReader extends IndexedRecordReaderTest {

		@Override
		protected Class<? extends SpyBuilder> getBuilderClass() {
			return CustomBuilder.class;
		}
	}

	public static class CustomBuilder extends IndexedRecordReaderTest.SpyBuilder {

		@Test
		public void empty(){}

		@Override
		protected IndexBuilder configure2(IndexBuilder b) {
			return b.primaryIndex().valueSerializer(StringCutSerializer.get(500));
		}
	}

}
