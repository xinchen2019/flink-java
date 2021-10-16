package com.apple.utils;

import java.io.*;

/**
 * @Program: flink-java
 * @ClassName: FileUtil
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-09-17 14:45
 * @Version 1.1.0
 **/
public class FileUtil {

    public void RecordToFile(String str, String filePath) {
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(filePath), true)
                    , "utf-8"));
            out.write(str);
            out.flush();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                out.close(); //释放资源
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
