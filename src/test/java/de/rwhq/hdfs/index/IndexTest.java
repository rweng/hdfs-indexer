package de.rwhq.hdfs.index;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.Collections.sort;
import static org.fest.assertions.Assertions.assertThat;

public abstract class IndexTest {
	private static Log LOG = LogFactory.getLog(IndexTest.class);
	private Index index;

	@VisibleForTesting
	static final Map<Long, String> map = new MapMaker().makeMap();

	static {
		map.put(0L, "1,Robin,25");
		map.put(10L, "2,Fritz,55");
		map.put(20L, "3,Roland,75");
		map.put(30L, "4,Herbert,105");
	}

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


	@Test(dependsOnMethods = "open")
	public void iteratorOnEmptyIndex() throws IOException {
		open();

		Iterator<String> i = index.getIterator();
		assertThat(i).isNotNull();
		assertThat(i.hasNext()).isFalse();
	}

	@Test(dependsOnMethods = "close")
	public void add2EntriesToIndex() throws IOException {
		open();

		List<Long> list = Lists.newArrayList(map.keySet());
		sort(list);
		list = list.subList(0, 2);
		for (Long key : list) {
			index.addLine(map.get(key), key);
		}

		index.close();
		index.open();

		assertThat(index.getMaxPos()).isEqualTo(10);
		Iterator<String> i = index.getIterator();
		assertThat(i.hasNext()).isTrue();
		assertThat(map.values()).contains(i.next());
		assertThat(map.values()).contains(i.next());
		assertThat(i.hasNext()).isFalse();
		assertThat(i.next()).isNull();

	}

	@Test(dependsOnMethods = "add2EntriesToIndex")
	public void multipleIndexes() throws IOException {
		add2EntriesToIndex();

		List<Long> list = Lists.newArrayList(map.keySet());
		sort(list);
		list = list.subList(2, 4);
		for (Long key : list) {
			index.addLine(map.get(key), key);
		}

		index.close();
		index.open();

		assertThat(index.getMaxPos()).isEqualTo(30);
		Iterator<String> i = index.getIterator();
		assertThat(i.hasNext()).isTrue();
		assertThat(map.values()).contains(i.next());
		assertThat(map.values()).contains(i.next());
		assertThat(map.values()).contains(i.next());
		assertThat(map.values()).contains(i.next());
		assertThat(i.hasNext()).isFalse();
		assertThat(i.next()).isNull();
	}
}
