package thesis;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.WritableComparable;

public class ComparableArrayList<T extends WritableComparable<? super T>> extends ArrayList<T>
		implements WritableComparable<ComparableArrayList<T>> {
	private static final long serialVersionUID = 1L;

	@Override
	public int compareTo(ComparableArrayList<T> other) {
		for (T o : other) {
			for (T e : this) {
				if (e.compareTo(o) != 0) {
					return e.compareTo(o);
				}
			}

		}
		return 0;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
	}
}
