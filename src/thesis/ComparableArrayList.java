package thesis;

import java.util.ArrayList;

public class ComparableArrayList<T extends Comparable<? super T>> extends ArrayList<T>
		implements Comparable<ComparableArrayList<T>> {
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
}
