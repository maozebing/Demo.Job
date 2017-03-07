package com.demo.common;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;

/**
 * 描述 ：
 *
 * @author : maozebing
 * @version : v1.00
 * @CreationDate : 2017/3/1 10:40
 * @Description :
 * @update : 修改人，修改时间，修改内容
 * @see :[相关类/方法]
 */
public class ConfigReader {
    public static void getTransTables(String configPath) {
        Properties p = new Properties();
        try {
            p.load(new FileInputStream(configPath));
            @SuppressWarnings("unchecked")
            Enumeration<String> em = (Enumeration<String>) p.propertyNames();
            String key = "";
            while (em.hasMoreElements()) {
                key = em.nextElement();
                Cache.transTables.put(key.toUpperCase(), p.getProperty(key));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
