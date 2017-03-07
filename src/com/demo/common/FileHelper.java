package com.demo.common;

import java.io.*;

/**
 * 描述 ：
 *
 * @author : maozebing
 * @version : v1.00
 * @CreationDate : 2017/3/1 20:29
 * @Description :
 * @update : 修改人，修改时间，修改内容
 * @see :[相关类/方法]
 */
public class FileHelper {
    /**
     * 移动文件至指定目录
     *
     * @param toPath
     * @param maxFiles
     */
    public static void moveFilesToDir(File[] sourceFiles, String toPath,
                                      int maxFiles) {
        if (sourceFiles == null)
            return;
        String toDir = toPath;
        File toFile;
        int index = 0;
        String newDirName = "";
        if (!toDir.endsWith(File.separator))
            toDir = toDir + File.separator;
        for (int i = sourceFiles.length - 1; i >= maxFiles; i--) {
            if (index == 0) {
                newDirName = toDir + String.valueOf(System.currentTimeMillis())
                        + File.separator;
                File dir = new File(newDirName);
                if (!dir.exists())
                    dir.mkdir();
            }
            if (sourceFiles[i].isFile()) {
                toFile = new File(newDirName + sourceFiles[i].getName());
                if (!toFile.exists())
                    sourceFiles[i].renameTo(toFile);
            }
            index++;
            if (index >= maxFiles) {
                index = 0;
            }
        }
    }

    /**
     * 移动文件至上级目录
     *
     * @param dir
     */
    public static void moveFilesToParentDir(File dir) {
        if (dir == null)
            return;
        String newDirName = dir.getParentFile().getAbsolutePath();
        File[] sourceFiles = dir.listFiles();
        File toFile;
        if (!newDirName.endsWith(File.separator))
            newDirName = newDirName + File.separator;
        for (int i = sourceFiles.length - 1; i >= 0; i--) {
            toFile = new File(newDirName + sourceFiles[i].getName());
            if (!toFile.exists())
                sourceFiles[i].renameTo(toFile);
        }
        dir.delete();
    }

    /**
     * 复制单个文件
     *
     * @param srcFileName
     *            待复制的文件名
     * @param destFileName
     *            目标文件名
     *            如果目标文件存在，是否覆盖
     * @return 如果复制成功返回true，否则返回false
     */
    public static boolean copyFile(String srcFileName, String destFileName) {
        File srcFile = new File(srcFileName);
        // 判断目标文件是否存在
        File destFile = new File(destFileName);
        if (destFile.exists()) {
            // 删除已经存在的目标文件，无论目标文件是目录还是单个文件
            new File(destFileName).delete();
        } else {
            // 如果目标文件所在目录不存在，则创建目录
            if (!destFile.getParentFile().exists()) {
                // 目标文件所在目录不存在
                if (!destFile.getParentFile().mkdirs()) {
                    // 复制文件失败：创建目标文件所在目录失败
                    return false;
                }
            }
        }

        // 复制文件
        int byteread = 0; // 读取的字节数
        InputStream in = null;
        OutputStream out = null;

        try {
            in = new FileInputStream(srcFile);
            out = new FileOutputStream(destFile);
            byte[] buffer = new byte[1024];

            while ((byteread = in.read(buffer)) != -1) {
                out.write(buffer, 0, byteread);
            }
            return true;
        } catch (FileNotFoundException e) {
            return false;
        } catch (IOException e) {
            return false;
        } finally {
            try {
                if (out != null)
                    out.close();
                if (in != null)
                    in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
