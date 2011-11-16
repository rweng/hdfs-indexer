package de.rwhq.hdfs.index;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static org.fest.assertions.Assertions.assertThat;

public abstract class AbstractMultiFileIndexTest {
	private static Log LOG = LogFactory.getLog(AbstractMultiFileIndexTest.class);
	private AbstractMultiFileIndex index;

	@BeforeMethod
	public void setup() {
		index = getNewIndex();
	}

	/**
	 * should be with {@code keyExtractor(new IntegerCSVExtractor(0, ","))}
	 *
	 * @return a new created index
	 */
	protected abstract AbstractMultiFileIndex getNewIndex();

	@Test
	public void open() throws IOException {
		assertThat(index.isOpen()).isFalse();
		index.open();
		assertThat(index.isOpen()).isTrue();
	}

	@Test(dependsOnMethods = "open")
	public void close() throws IOException {
		open();
		index.close();
		assertThat(index.isOpen()).isFalse();
	}


	@Test(dependsOnMethods = "addingStuffToIndex")
	public void iteratorOnEmptyIndex() throws IOException {
		open();

		Iterator<String> i = index.getIterator();
		assertThat(i).isNotNull();
		assertThat(i.hasNext()).isFalse();
	}

	@Test(dependsOnMethods = "close")
	public void addingStuffToIndex() throws IOException {
		open();

		List<String> list = Lists.newArrayList();
		list.add("1,Robin,25");
		list.add("2,Fritz,55");

		index.addLine(list.get(0), 0);
		index.addLine(list.get(1), 10);
		index.close();
		index.open();

		assertThat(index.getMaxPos()).isEqualTo(10);
		Iterator<String> i = index.getIterator(false);
		assertThat(i.hasNext()).isTrue();
		assertThat(list).contains(i.next());
		assertThat(list).contains(i.next());
		assertThat(i.hasNext()).isFalse();
		assertThat(i.next()).isNull();

		// ensure lock if is deleted after close
		index.close();
		assertThat(index.getLockFile()).doesNotExist();
	}
}
