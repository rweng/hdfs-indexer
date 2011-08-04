package com.freshbourne.hdfs.index;

/**
 * just a container class
 * @param <K>
 * @param <V>
 */
public class KeyValue<K, V> {
	public K key;
	public V value;

	public KeyValue(){}

	public KeyValue(K key, V value){
		this.key = key;
		this.value = value;
	}
}
