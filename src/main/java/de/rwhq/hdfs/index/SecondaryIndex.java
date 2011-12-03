package de.rwhq.hdfs.index;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import de.rwhq.btree.Range;
import de.rwhq.hdfs.index.extractor.ExtractionException;
import de.rwhq.serializer.LongSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;

import javax.annotation.Nullable;
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

	private Configuration   jobConf;
	private int             maxLineLength;
	private Text            text;
	private DataInputStream dataStream;

	@VisibleForTesting
	InputStreamReader inReader;

	public SecondaryIndex(MFIBuilder b) {
		super(b.valueSerializer(LongSerializer.INSTANCE));

		checkArgument(b.getSecondaryIndexReadBufferSize() > 0, "secondary index read buffer size must be > 0");

		this.inputStream = b.getInputStream();
		this.jobConf = b.getJobConfiguration();
		this.text = new Text();
	}

	@Override
	protected AbstractMap.SimpleEntry<K, Long> extractEntry(String line, long pos) throws ExtractionException {
		return new AbstractMap.SimpleEntry<K, Long>(keyExtractor.extract(line), pos);
	}

	private void ensureIteratorRequirements() {
		checkNotNull(inputStream, "inputStream must not be null for iterating over a secondary index");
		checkNotNull(jobConf, "job configuration must not be null for iterating over a secondary index");


		inReader = new InputStreamReader(inputStream);

		maxLineLength = jobConf.getInt("mapred.linerecordreader.maxlength",
				Integer.MAX_VALUE);
	}

	@Override
	public Iterator<String> getIterator() {
		ensureIteratorRequirements();
		return super.getIterator();
	}

	@Override
	public Iterator<String> getIterator(Range<Long> range) throws IOException {
		ensureIteratorRequirements();

		return Iterators.transform(getTreeIterator(range), new Function<Long, String>() {
			@Override
			public String apply(@Nullable Long input) {

				try {
					long oldPos = inputStream.getPos();
					inputStream.seek(input);
					String result = new BufferedReader(inReader).readLine();
					inputStream.seek(oldPos);
					return result;
				} catch (IOException e) {
					throw new RuntimeException("error when reading from inputStream", e);
				}
			}
		});
	}
}
