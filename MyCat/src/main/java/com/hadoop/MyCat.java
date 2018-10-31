package com.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.net.URI;

/**
 * @author peicong
 * @date 2018/10/30 0030
 */
public class MyCat {

  public static void main(String[] args) throws Exception {
    String uri = args[0];
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(uri), conf);
    InputStream inputStream = null;
    try {
      inputStream = fs.open(new Path(uri));
      IOUtils.copyBytes(inputStream, System.out, 4096, false);
    } finally {
      IOUtils.closeStream(inputStream);
    }
  }
}
