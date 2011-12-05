package de.rwhq.hdfs.index;

import de.rwhq.btree.Range;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
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
		protected MFIBuilder configureBuilder(MFIBuilder b) {
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
		protected MFIBuilder configure2(MFIBuilder b) {
			return b.secondaryIndex()
					.addDefaultRange(new Range(1, 4))
					.addDefaultRange(new Range(10, 10));
		}
	}
}
