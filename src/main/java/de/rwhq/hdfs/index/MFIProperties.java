package de.rwhq.hdfs.index;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;

import java.io.*;
import java.util.List;

public class MFIProperties implements Serializable {
	private static final long serialVersionUID = 1L;

	private String path;
	private List<MFIProperty> properties;

	public static class MFIProperty implements Serializable{
		private static final long serialVersionUID = 1L;

		private String filePath;
		private Long startPos;
		private Long endPos;

		public MFIProperty(){}
		public MFIProperty(String filePath, Long startPos, Long endPos){
			this.filePath = filePath;
			this.startPos = startPos;
			this.endPos = endPos;
		}

		@Override
		public boolean equals(Object other){
			if(other instanceof MFIProperty){
				MFIProperty p2 = (MFIProperty) other;
				return Objects.equal(filePath, p2.filePath)
						&& Objects.equal(startPos, p2.startPos)
						&& Objects.equal(endPos, p2.endPos);
			} else {
				return false;
			}
		}

		@Override
		public int hashCode(){
			return Objects.hashCode(filePath, startPos, endPos);
		}
	}

	public MFIProperties(String path){
		this.path = path;
		this.properties = Lists.newArrayList();
	}

	public List<MFIProperty> asList(){
		return properties;
	}

	public void write() throws IOException {
		FileOutputStream stream = new FileOutputStream(path);
		ObjectOutputStream oStream = new ObjectOutputStream(stream);

		oStream.writeObject(this);

		oStream.close();
	}

	public void read() throws IOException {
		FileInputStream file = new FileInputStream(path);
		ObjectInputStream oStream = new ObjectInputStream(file);


		MFIProperties loaded;
		
		try {
			loaded = (MFIProperties) oStream.readObject();
		} catch (ClassNotFoundException e) {
			throw new IOException("error when reading object", e);
		}

		properties = loaded.asList();
	}
}
