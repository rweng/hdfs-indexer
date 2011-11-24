package de.rwhq.hdfs.index;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.fest.assertions.Assertions.assertThat;

public class MFIPropertiesTest {
	private MFIProperties properties;

	@Before
	public void setUp(){
		properties = new MFIProperties("/tmp/mfipropertiestest");
		List<MFIProperties.MFIProperty> list = properties.asList();
		list.add(new MFIProperties.MFIProperty("/a", 0L, 100L));
		list.add(new MFIProperties.MFIProperty("/b", 100L, 200L));
	}

	@Test
	public void write() throws IOException {
		properties.write();
		MFIProperties p = new MFIProperties("/tmp/mfipropertiestest");
		p.read();

		assertThat(p.asList()).isEqualTo(properties.asList());
	}
}
