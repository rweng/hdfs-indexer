package de.rwhq.hdfs.index.extractor;

import de.rwhq.hdfs.index.extractor.ExtractionException;
import de.rwhq.hdfs.index.extractor.IntegerCSVExtractor;
import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

public class IntegerCSVExtractorTest {
	private IntegerCSVExtractor extractor;

	@Before
	public void setUp() throws Exception {
		extractor = new IntegerCSVExtractor(1, "[ \t|]+");
	}

	@Test
	public void testExtract() throws Exception {
		assertThat(extractor.extract("1 2 3")).isEqualTo(2);
		assertThat(extractor.extract("5\t6\t7")).isEqualTo(6);
		assertThat(extractor.extract("1|2|3")).isEqualTo(2);
	}

	@Test
	public void testGetId() throws Exception {
		assertThat(extractor.getId()).isEqualTo("1");
	}

	@Test
	public void tpch() throws ExtractionException {
		extractor = new IntegerCSVExtractor(0, "\\|");
		String test =
				"196|135052|79|1|19|20653.95|0.03|0.02|R|F|1993-04-17|1993-05-27|1993-04-30|NONE|SHIP|sts maintain foxes. furiously regular p|";
		assertThat(extractor.extract(test)).isEqualTo(196);
	}
}
