package org.apache.hadoop.ozone.container.common.helpers;

import org.apache.hadoop.ozone.OzoneConsts;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Properties;

/**
 * This is a utility class which helps to create the rocksdb count file
 * on datanode.
 */
public class RocksDBCountFile {
  private final int rocksdbCount;
  private final long createTime;
  private final long updateTime;

  public RocksDBCountFile(
      int rocksdbCount, long createTime, long updateTime) {
    this.rocksdbCount = rocksdbCount;
    this.createTime = createTime;
    this.updateTime = updateTime;
  }

  private Properties createProperties() {
    Properties properties = new Properties();
    properties.setProperty(
        OzoneConsts.ROCKSDB_COUNT,
        String.valueOf(rocksdbCount));
    properties.setProperty(
        OzoneConsts.ROCKSDB_COUNT_FILE_CREATE_TIME,
        String.valueOf(createTime));
    properties.setProperty(
        OzoneConsts.ROCKSDB_COUNT_FILE_UPDATE_TIME,
        String.valueOf(updateTime));

    return properties;
  }

  /**
   * Creates a rocksdb count File in specified path.
   * @param path
   * @throws IOException
   */
  public void createRocksDBCountFile(File path) throws
      IOException {
    try (RandomAccessFile file = new RandomAccessFile(path, "rws");
         FileOutputStream out = new FileOutputStream(file.getFD())) {
      file.getChannel().truncate(0);
      Properties properties = createProperties();
      /*
       * If server is interrupted before this line,
       * the version file will remain unchanged.
       */
      properties.store(out, null);
    }
  }

  /**
   * Creates a property object from the specified file content.
   * @param  rocksDBCountFile
   * @return Properties
   * @throws IOException
   */
  public static Properties readFrom(File rocksDBCountFile) throws IOException {
    try (RandomAccessFile file = new RandomAccessFile(rocksDBCountFile, "rws");
         FileInputStream in = new FileInputStream(file.getFD())) {
      Properties props = new Properties();
      props.load(in);
      return props;
    }
  }
}
