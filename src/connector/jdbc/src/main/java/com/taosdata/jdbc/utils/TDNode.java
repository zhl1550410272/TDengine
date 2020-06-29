package com.taosdata.jdbc.utils;

public class TDNode {
    
    private int index;
    private int running;
    private int deployed;
    private boolean testCluster;
    private int valgrind;
    private String path;

    public TDNode(int index) {
        this.index = index;
        running = 0;
        deployed = 0;
        testCluster = false;
        valgrind = 0;        
    }

    



}