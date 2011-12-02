package de.rwhq.hdfs.index;

import de.rwhq.btree.Range;
import de.rwhq.comparator.IntegerComparator;
import de.rwhq.serializer.IntegerSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.LineReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;

import static org.mockito.Mockito.mock;

@RunWith(Enclosed.class)
public class SecondaryIndexTest {

	public static class Main extends AbstractMultiFileIndexTest {
		private Configuration conf    = new Configuration();
		private Path          tmpFile = new Path("/tmp/SecondaryIndexTestTmpFile");
		private FSDataInputStream input;
		private boolean before = false;
		private LocalFileSystem fs;

		@After
		public void tearDown() throws IOException {
			fs.close();
			fs = null;
		}

		@Before
		public void setUpSecondary() throws IOException {
			fs = FileSystem.getLocal(new Configuration());
		}

		@Override
		protected void addToIndexInputStream(AbstractMultiFileIndex index, String line, long pos) {
			try {
				RandomAccessFile file = new RandomAccessFile(new File(tmpFile.toString()), "rw");
				file.seek(pos);
				file.write(line.getBytes());
				file.close();

				SecondaryIndex sindex = (SecondaryIndex) index;
				input = sindex.inputStream = fs.open(tmpFile);
				sindex.inReader = new InputStreamReader(sindex.inputStream);

			} catch (IOException e) {
				throw new RuntimeException(e);
			}

		}

		@Override
		protected BTreeIndexBuilder configureBuilder(BTreeIndexBuilder b) {
			if (input == null)
				input = mock(FSDataInputStream.class);

			return b.secondaryIndex().inputStream(input).jobConfiguration(conf);
		}
	}


	public static class SecondarIndexedRecordReader extends IndexedRecordReaderTest {

		@Override
		protected Class<? extends SpyBuilder> getBuilderClass() {
			return CustomBuilder.class;
		}
	}


	public static class CustomBuilder extends IndexedRecordReaderTest.SpyBuilder {

		@Test
		public void empty(){}

		@Override
		public BTreeIndexBuilder configure(BTreeIndexBuilder bTreeIndexBuilder) {
			return bTreeIndexBuilder
					.indexFolder(IndexedRecordReaderTest.INDEX)
					.addDefaultRange(new Range(1, 4))
					.cacheSize(10)
					.secondaryIndex()
					.keySerializer(IntegerSerializer.INSTANCE)
					.keyExtractor(new IntegerCSVExtractor(0, ","))
					.comparator(IntegerComparator.INSTANCE);
		}
	}
}
