package com.taosdata.jdbc;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.junit.BeforeClass;

public class BaseTest {
    
    @BeforeClass
    public static void setupEnv() {
                
        String path = System.getProperty("user.dir");            
        String[] scripts = new String[]{"python3", path + "/../../../tests/pytest/test.py", "-m", "127.0.0.1"};
        
        try{        
            Process ps = Runtime.getRuntime().exec(scripts);
            ps.waitFor();

            BufferedReader br = new BufferedReader(new InputStreamReader(ps.getInputStream()));
            while(br.readLine() != null) {
                System.out.println(br.readLine());
            }            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}