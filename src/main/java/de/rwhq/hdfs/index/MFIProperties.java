package de.rwhq.hdfs.index;

import com.google.common.base.Objects;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;

import java.io.*;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class MFIProperties implements Serializable {
	private static final long serialVersionUID = 1L;

	private String path;
	private List<MFIProperty> properties;

	public long getMaxPos() {
		long pos = -1;

		for(MFIProperty p : properties){
			if(p.endPos > pos)
				pos = p.endPos;
		}

		return pos;
	}

	public int removeByPath(String path) {
		checkNotNull(path);

		int removed = 0;

		Iterator<MFIProperty> iterator = properties.iterator();
		while(iterator.hasNext()){
			MFIProperty next = iterator.next();
			if(path.equals(next.filePath)){
				iterator.remove();
				removed++;
			}
		}

		return removed;
	}

	public boolean exists() {
		return new File(path).exists();
	}

	public static class MFIProperty implements Serializable{
		private static final long serialVersionUID = 1L;

		public String filePath;
		public Long startPos;
		public Long endPos;

		/**
		 * for serialization only
		 */
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
		public String toString(){
			return Objects.toStringHelper(this)
					.add("filePath", filePath)
					.add("startPos", startPos)
					.add("endPos", endPos)
					.toString();
		}

		@Override
		public int hashCode(){
			return Objects.hashCode(filePath, startPos, endPos);
		}

		public File getFile() {
			return new File(filePath);
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

		// ensure all MFIProperties have all values set
		for(MFIProperty p : properties){
			checkNotNull(p.filePath, "All attributes of MFIProperty must be set for writing %s", toString());
			checkNotNull(p.startPos, "All attributes of MFIProperty must be set for writing %s", toString());
			checkNotNull(p.endPos, "All attributes of MFIProperty must be set for writing %s", toString());

			checkState(p.startPos < p.endPos, "MFIProperty.startPos must be < MFIProperty.endPos for writing %s", toString());
		}

		FileOutputStream stream = new FileOutputStream(path);
		ObjectOutputStream oStream = new ObjectOutputStream(stream);

		oStream.writeObject(this);

		oStream.close();
	}

	@Override
	public String toString(){
		return Objects.toStringHelper(this)
				.add("path", path)
				.add("properties", properties)
				.toString();
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

	public static MFIProperties read(String path) throws IOException {
		MFIProperties p = new MFIProperties(path);
		p.read();
		return p;
	}
}
