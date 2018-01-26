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
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HDFSSequenceFile extends AbstractHDFSWriter {

  private static final Logger logger =
      LoggerFactory.getLogger(HDFSSequenceFile.class);
  private SequenceFile.Writer writer;
  private String writeFormat;
  private Context serializerContext;
  private SequenceFileSerializer serializer;
  private boolean useRawLocalFileSystem;
  private FSDataOutputStream outStream = null;

  public HDFSSequenceFile() {
    writer = null;
  }

  @Override
  public void configure(Context context) {
    super.configure(context);

    // use binary writable serialize by default
    //获取写入格式writeFormat"，默认格式是二进制的Writable(HDFSWritableSerializer.Builder.class)
    writeFormat = context.getString("hdfs.writeFormat",
      SequenceFileSerializerType.Writable.name());
    //再获取是否使用HDFS本地文件系统"hdfs.useRawLocalFileSystem"，默认是flase不使用
    useRawLocalFileSystem = context.getBoolean("hdfs.useRawLocalFileSystem",
        false);
    //获取writeFormat的所有配置信息serializerContext
    serializerContext = new Context(
            context.getSubProperties(SequenceFileSerializerFactory.CTX_PREFIX));
    //根据writeFormat和serializerContext构造SequenceFileSerializer的对象serializer
    serializer = SequenceFileSerializerFactory
            .getSerializer(writeFormat, serializerContext);
    logger.info("writeFormat = " + writeFormat + ", UseRawLocalFileSystem = "
        + useRawLocalFileSystem);
  }

  @Override
  public void open(String filePath) throws IOException {
    open(filePath, null, CompressionType.NONE);
  }

  //open(String filePath)会调用open(filePath, null, CompressionType.NONE)压缩方法，只不过没有压缩类
  //压缩open方法先判断是否使用了本地文件系统，然后根据hadoop的配置信息是否支持追加"hdfs.append.support"，
  // 构造相应的SequenceFile即writer
  @Override
  public void open(String filePath, CompressionCodec codeC,
      CompressionType compType) throws IOException {
    Configuration conf = new Configuration();
    Path dstPath = new Path(filePath);
    FileSystem hdfs = dstPath.getFileSystem(conf);
    open(dstPath, codeC, compType, conf, hdfs);
  }

  protected void open(Path dstPath, CompressionCodec codeC,
      CompressionType compType, Configuration conf, FileSystem hdfs)
          throws IOException {
    if (useRawLocalFileSystem) {
      if (hdfs instanceof LocalFileSystem) {
        hdfs = ((LocalFileSystem)hdfs).getRaw();
      } else {
        logger.warn("useRawLocalFileSystem is set to true but file system " +
            "is not of type LocalFileSystem: " + hdfs.getClass().getName());
      }
    }
    if (conf.getBoolean("hdfs.append.support", false) == true && hdfs.isFile(dstPath)) {
      outStream = hdfs.append(dstPath);
    } else {
      outStream = hdfs.create(dstPath);
    }
    writer = SequenceFile.createWriter(conf, outStream,
        serializer.getKeyClass(), serializer.getValueClass(), compType, codeC);

    registerCurrentStream(outStream, hdfs, dstPath);
  }

  @Override
  public void append(Event e) throws IOException {
    /**(1)serializer为HDFSWritableSerializer时，则Key会是event.getHeaders().get("timestamp")，
     *    如果没有"timestamp"的Headers则使用当前系统时间System.currentTimeMillis()，然后将时间封装成LongWritable;
     *    Value是将event.getBody()封装成BytesWritable，代码是bytesObject.set(e.getBody(), 0, e.getBody().length)；
     * (2)serializer为HDFSTextSerializer时，Key和上述HDFSWritableSerializer一样；
     *    Value会将event.getBody()封装成Text，代码是textObject.set(e.getBody(), 0, e.getBody().length)。*
     */
    for (SequenceFileSerializer.Record record : serializer.serialize(e)) {
      writer.append(record.getKey(), record.getValue());
    }
  }

  @Override
  public void sync() throws IOException {
    writer.sync();
    hflushOrSync(outStream);
  }

  @Override
  public void close() throws IOException {
    writer.close();
    outStream.close();
    unregisterCurrentStream();
  }
}
