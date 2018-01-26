/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.sink.hdfs;

import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.serialization.EventSerializerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HDFSDataStream extends AbstractHDFSWriter {

  private static final Logger logger = LoggerFactory.getLogger(HDFSDataStream.class);

  private FSDataOutputStream outStream;
  private String serializerType;
  private Context serializerContext;
  private EventSerializer serializer;
  private boolean useRawLocalFileSystem;

  @Override
  public void configure(Context context) {
    super.configure(context);

    //先获取serializerType类型，默认是TEXT(BodyTextEventSerializer.Builder.class)
    //此外还有HEADER_AND_TEXT(HeaderAndBodyTextEventSerializer.Builder.class)、OTHER(null)、
    // AVRO_EVENT(FlumeEventAvroEventSerializer.Builder.class)共四种类型
    serializerType = context.getString("serializer", "TEXT");
    //获取是否使用HDFS本地文件系统"hdfs.useRawLocalFileSystem"，默认是flase不使用
    useRawLocalFileSystem = context.getBoolean("hdfs.useRawLocalFileSystem",
        false);
    //获取serializer的所有配置信息serializerContext
    serializerContext =
        new Context(context.getSubProperties(EventSerializer.CTX_PREFIX));
    logger.info("Serializer = " + serializerType + ", UseRawLocalFileSystem = "
        + useRawLocalFileSystem);
  }

  @VisibleForTesting
  protected FileSystem getDfs(Configuration conf, Path dstPath) throws IOException {
    return dstPath.getFileSystem(conf);
  }

  protected void doOpen(Configuration conf, Path dstPath, FileSystem hdfs) throws IOException {
    if (useRawLocalFileSystem) {
      if (hdfs instanceof LocalFileSystem) {
        hdfs = ((LocalFileSystem)hdfs).getRaw();
      } else {
        logger.warn("useRawLocalFileSystem is set to true but file system " +
            "is not of type LocalFileSystem: " + hdfs.getClass().getName());
      }
    }

    boolean appending = false;
    // 构造FSDataOutputStream，作为属性outStream
    if (conf.getBoolean("hdfs.append.support", false) == true && hdfs.isFile(dstPath)) {
      outStream = hdfs.append(dstPath);
      appending = true;
    } else {
      outStream = hdfs.create(dstPath);
    }

    // 初始化Serializer
    serializer = EventSerializerFactory.getInstance(
        serializerType, serializerContext, outStream);
    if (appending && !serializer.supportsReopen()) {
      outStream.close();
      serializer = null;
      throw new IOException("serializer (" + serializerType +
          ") does not support append");
    }

    // must call superclass to check for replication issues
    registerCurrentStream(outStream, hdfs, dstPath);

    if (appending) {
      serializer.afterReopen();
    } else {
      serializer.afterCreate();
    }
  }

  @Override
  public void open(String filePath) throws IOException {
    Configuration conf = new Configuration();
    // 构造hdfs路径
    Path dstPath = new Path(filePath);
    FileSystem hdfs = getDfs(conf, dstPath);
    // 调用doOpen方法
    doOpen(conf, dstPath, hdfs);
  }

  @Override
  public void open(String filePath, CompressionCodec codec,
                   CompressionType cType) throws IOException {
    open(filePath);
  }

  @Override
  public void append(Event e) throws IOException {
    /**
     * 直接使用serializer的write方法
       serializer是org.apache.flume.serialization.EventSerializer接口的实现类
       (1)serializer为BodyTextEventSerializer(default)，
          则其write(e)方法会将e.getBody()写入输出流，并根据配置再写入一个"\n"；
     　(2)serializer为HeaderAndBodyTextEventSerializer,
         则其write(e)方法会将e.getHeaders() + " "(注意此空格)和e.getBody()写入输出流，并根据配置再写入一个"\n"；
     　(3)serializer为FlumeEventAvroEventSerializer，则其write(e)方法会将event整体写入dataFileWriter。
     */
    serializer.write(e);
  }

  @Override
  public void sync() throws IOException {
    serializer.flush();
    outStream.flush();
    hflushOrSync(outStream);
  }

  @Override
  public void close() throws IOException {
    serializer.flush();
    serializer.beforeClose();
    outStream.flush();
    hflushOrSync(outStream);
    outStream.close();

    unregisterCurrentStream();
  }

}
