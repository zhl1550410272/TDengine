package com.taosdata.jdbc.utils;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class TDSimClient {
    
    private boolean testCluster;
    private String path;
    private String cfgDir;
    private String logDir;
    private String cfgPath;
    
    public TDSimClient() {
        testCluster = false;
    }

    public void setTestCluster(boolean testCluster) {
        this.testCluster = testCluster;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public void setCfgConfig(String option, String value) {
        String cmd = "echo " + option + " " +  value + " >> " + this.cfgPath;
        
        try {
            Process ps = Runtime.getRuntime().exec(cmd);            

            BufferedReader br = new BufferedReader(new InputStreamReader(ps.getInputStream()));
            while(br.readLine() != null) {
                System.out.println(br.readLine());
            }  
            
            ps.waitFor();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void deploy() {
        this.logDir = this.path + "/sim/psim/log";
        this.cfgDir = this.path + "/sim/psim/cfg";
        this.cfgPath = this.path + "/sim/psim/cfg/taos.cfg";

        try {
            String cmd = "rm -rf " + this.logDir;
            Runtime.getRuntime().exec(cmd).waitFor();
            
            cmd = "rm -rf " + this.cfgDir;
            Runtime.getRuntime().exec(cmd).waitFor();

            

        }
    }


}