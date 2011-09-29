package com.freshbourne.hdfs.index;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;


public class BTreeIndexTest {

    private BTreeIndex index;

    private static Injector injector;

    static {
        injector = Guice.createInjector(new IndexModule());
    }

    @Before
    public void setUp(){
        index = injector.getInstance(BTreeIndex.class);
    }

    @Test
    public void creation(){
        assertTrue( index instanceof BTreeIndex );
    }



}
