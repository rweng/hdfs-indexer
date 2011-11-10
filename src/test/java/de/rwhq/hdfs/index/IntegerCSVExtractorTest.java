package de.rwhq.hdfs.index;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.fest.assertions.Assertions.assertThat;

public class IntegerCSVExtractorTest {
	private IntegerCSVExtractor extractor;

	@BeforeMethod
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
}
