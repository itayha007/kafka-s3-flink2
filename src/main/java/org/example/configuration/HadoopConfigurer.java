package org.example.configuration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;

public class HadoopConfigurer {
    public static Configuration hdfsConfiguration(){
        Configuration configuration = new Configuration();
        configuration.addResource(new Path(new File("./config/hdfs-site.xml").getAbsolutePath()));
        configuration.addResource(new Path(new File("./config/core-site.xml").getAbsolutePath()));

        return configuration;
    }
}
