package com.demo.main;

import java.io.File;

import com.demo.common.Cache;
import com.demo.common.ConfigReader;
import com.demo.job.impl.OracleToMongoDB;
import org.tanukisoftware.wrapper.WrapperListener;
import org.tanukisoftware.wrapper.WrapperManager;

import com.demo.common.Constants;
import com.demo.common.XmlReader;
import com.demo.entity.JobConfig;
import com.demo.job.JobManager;

public class App {

    public static void main(String[] args) {
        try {
            String appdir = System.getProperty("user.dir");
            File file = new File(appdir);
            String parent = file.getParent();
            File parentPath = new File(parent);
            System.out.println(appdir);
            // 设置系统变量
            Constants.APPLICATION_ROOT_DIR = "D:\\Code\\trans\\conf";

            //初始化调度服务器
            JobManager.init();

            //读取数据库配置文件/数据导出配置文件/数据导入配置文件/JOB配置文件
            initConfig();

            //初始化job
            for (JobConfig config : Cache.jobConfigs) {
                try {
                    if (config.isActivity()) {
                        JobManager.addJob(config);
                        System.out.println("任务实例[" + config.getName() + "]已加入调度.");
                    } else {
                        System.out.println(config.getName() + " 任务实例Activity=false 不进行处理");
                    }
                } catch (Exception cve) {
                    //配置错误忽略这个任务
                    System.out.println(cve);
                }
            }

            //启动调度服务器
            JobManager.start();

            //执行手动数据导出导入程序
            new OracleToMongoDB().execute();

            //启动wrapper服务
            //WrapperManager.start(new App(), args);

        } catch (Exception e) {

        }
    }

    /**
     * 初始化配置
     */
    private static void initConfig() {
        if (Cache.transTables.size() == 0) {
            ConfigReader.getTransTables(Constants.APPLICATION_ROOT_DIR + File.separatorChar + "DBConfig.ini");
        }
        if (Cache.outTableList.size() == 0) {
            XmlReader.getOutRootXml(Constants.APPLICATION_ROOT_DIR + File.separatorChar + "transTablesOut.xml");
        }
        if (Cache.inTableList.size() == 0) {
            XmlReader.getInRootXml(Constants.APPLICATION_ROOT_DIR + File.separatorChar + "transTablesIn.xml");
        }
        if (Cache.mongodbTableList.size() == 0) {
            try {
                XmlReader.getMongodbRootXml(Constants.APPLICATION_ROOT_DIR + File.separatorChar + "mongodbTables.xml");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (Cache.jobConfigs.size() == 0) {
            XmlReader.getJobConfigXml(Constants.APPLICATION_ROOT_DIR + File.separatorChar + "job-config.xml");
        }
    }

}
