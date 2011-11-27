package de.rwhq.hdfs.index;

import com.google.common.annotations.VisibleForTesting;
import de.rwhq.serializer.LongSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.AbstractMap;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class SecondaryIndex<K> extends AbstractMultiFileIndex<K, Long> {

	@VisibleForTesting
	FSDataInputStream inputStream;
	
	private Configuration     jobConf;
	private int maxLineLength;
	private Text text;
	private DataInputStream dataStream;

	@VisibleForTesting
	InputStreamReader inReader;

	public SecondaryIndex(BTreeIndexBuilder b) {
		super(b, LongSerializer.INSTANCE);

		checkArgument(b.getSecondaryIndexReadBufferSize() > 0, "secondary index read buffer size must be > 0");

		this.inputStream = b.getInputStream();
		this.jobConf = b.getJobConfiguration();
		this.text = new Text();
	}

	@Override
	protected AbstractMap.SimpleEntry<K, Long> extractEntry(String line, long pos) throws ExtractionException {
		return new AbstractMap.SimpleEntry<K, Long>(keyExtractor.extract(line), pos);
	}

	@Override
	public Iterator<String> getIterator() {
		checkNotNull(inputStream, "inputStream must not be null for iterating over a secondary index");
		checkNotNull(jobConf, "job configuration must not be null for iterating over a secondary index");

		inReader = new InputStreamReader(inputStream);

		maxLineLength = jobConf.getInt("mapred.linerecordreader.maxlength",
				Integer.MAX_VALUE);
		
		return new SecondaryIndexIterator(getIterator(true));
	}

	class SecondaryIndexIterator implements Iterator<String> {

		private Iterator<Long> iterator;

		public SecondaryIndexIterator(Iterator<Long> iterator) {
			this.iterator = iterator;
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public String next() {
			Long nextLong = iterator.next();

			if (nextLong == null)
				return null;


			try {
				long oldPos = inputStream.getPos();
				inputStream.seek(nextLong);
				text.set(new BufferedReader(inReader).readLine());
				inputStream.seek(oldPos);
			} catch (IOException e) {
				throw new RuntimeException("error when reading from inputStream", e);
			}

			return text.toString();
		}

		@Override
		public void remove() {
			iterator.remove();
		}
	}
}
