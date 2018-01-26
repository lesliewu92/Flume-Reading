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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.flume.Channel;
import org.apache.flume.Clock;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.SystemClock;
import org.apache.flume.Transaction;
import org.apache.flume.auth.FlumeAuthenticationUtil;
import org.apache.flume.auth.PrivilegedExecutor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.formatter.output.BucketPath;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * HDFSEventSink用于把数据从channel中拿出来（主动pull的形式）然后放到hdfs中
 */
public class HDFSEventSink extends AbstractSink implements Configurable {
  public interface WriterCallback {
    public void run(String filePath);
  }

  private static final Logger LOG = LoggerFactory.getLogger(HDFSEventSink.class);

  private static String DIRECTORY_DELIMITER = System.getProperty("file.separator");

  private static final long defaultRollInterval = 30;
  private static final long defaultRollSize = 1024;
  private static final long defaultRollCount = 10;
  private static final String defaultFileName = "FlumeData";
  private static final String defaultSuffix = "";
  private static final String defaultInUsePrefix = "";
  private static final String defaultInUseSuffix = ".tmp";
  private static final long defaultBatchSize = 100;
  private static final String defaultFileType = HDFSWriterFactory.SequenceFileType;
  private static final int defaultMaxOpenFiles = 5000;
  // Time between close retries, in seconds
  private static final long defaultRetryInterval = 180;
  // Retry forever.
  private static final int defaultTryCount = Integer.MAX_VALUE;

  /**
   * Default length of time we wait for blocking BucketWriter calls
   * before timing out the operation. Intended to prevent server hangs.
   */
  private static final long defaultCallTimeout = 10000;
  /**
   * Default number of threads available for tasks
   * such as append/open/close/flush with hdfs.
   * These tasks are done in a separate thread in
   * the case that they take too long. In which
   * case we create a new file and move on.
   */
  private static final int defaultThreadPoolSize = 10;
  private static final int defaultRollTimerPoolSize = 1;

  private final HDFSWriterFactory writerFactory;
  /**
   * WriterLinkedHashMap是LinkedHashMap的子类
   * 用来存放文件到BucketWriter的对应关系，在start方法中初始化
   */
  private WriterLinkedHashMap sfWriters;

  private long rollInterval;
  private long rollSize;
  private long rollCount;
  private long batchSize;
  private int threadsPoolSize;
  private int rollTimerPoolSize;
  private CompressionCodec codeC;
  private CompressionType compType;
  private String fileType;
  private String filePath;
  private String fileName;
  private String suffix;
  private String inUsePrefix;
  private String inUseSuffix;
  private TimeZone timeZone;
  private int maxOpenFiles;
  private ExecutorService callTimeoutPool;
  private ScheduledExecutorService timedRollerPool;

  private boolean needRounding = false;
  private int roundUnit = Calendar.SECOND;
  private int roundValue = 1;
  private boolean useLocalTime = false;

  private long callTimeout;
  private Context context;
  private SinkCounter sinkCounter;

  private volatile int idleTimeout;
  private Clock clock;
  private FileSystem mockFs;
  private HDFSWriter mockWriter;
  private final Object sfWritersLock = new Object();
  private long retryInterval;
  private int tryCount;
  private PrivilegedExecutor privExecutor;


  /*
   * Extended Java LinkedHashMap for open file handle LRU queue.
   * We want to clear the oldest file handle if there are too many open ones.
   */
  private static class WriterLinkedHashMap
      extends LinkedHashMap<String, BucketWriter> {

    private final int maxOpenFiles;

    public WriterLinkedHashMap(int maxOpenFiles) {
      super(16, 0.75f, true); // stock initial capacity/load, access ordering
      this.maxOpenFiles = maxOpenFiles;
    }

    @Override
    protected boolean removeEldestEntry(Entry<String, BucketWriter> eldest) {
      if (size() > maxOpenFiles) {
        // If we have more that max open files, then close the last one and
        // return true
        try {
          eldest.getValue().close();
        } catch (IOException e) {
          LOG.warn(eldest.getKey().toString(), e);
        } catch (InterruptedException e) {
          LOG.warn(eldest.getKey().toString(), e);
          Thread.currentThread().interrupt();
        }
        return true;
      } else {
        return false;
      }
    }
  }

  //构造函数声明一个HDFSWriterFactory对象
  //在后面会使用HDFSWriterFactory的getWriter方法会根据file类型返回对应的HDFSWriter实现类
  public HDFSEventSink() {
    this(new HDFSWriterFactory());
  }

  public HDFSEventSink(HDFSWriterFactory writerFactory) {
    this.writerFactory = writerFactory;
  }

  @VisibleForTesting
  Map<String, BucketWriter> getSfWriters() {
    return sfWriters;
  }

  // read configuration and setup thresholds
  //主要用来加载配置文件,及初始化一些重要参数
  @Override
  public void configure(Context context) {
    this.context = context;

    //HDFS目录路径 这样要把Hadoop的配置文件放到classpath
    filePath = Preconditions.checkNotNull(
        context.getString("hdfs.path"), "hdfs.path is required");
    //HDFS目录中，由Flume创建的文件前缀。
    fileName = context.getString("hdfs.filePrefix", defaultFileName);//FlumeData
    //文件后缀
    this.suffix = context.getString("hdfs.fileSuffix", defaultSuffix);//""
    //文件正在写入时的前缀。
    inUsePrefix = context.getString("hdfs.inUsePrefix", defaultInUsePrefix);//""
    //文件正在写入时的后缀。
    inUseSuffix = context.getString("hdfs.inUseSuffix", defaultInUseSuffix);//.tmp
    //时区的名称，主要用来解决目录路径。timeZone 默认值：Local Time
    String tzName = context.getString("hdfs.timeZone");
    timeZone = tzName == null ? null : TimeZone.getTimeZone(tzName);
    //当前写入的文件滚动间隔，默认30秒生成一个新的文件 (设置为0的话表示不根据时间close hdfs文件)
    //hdfs sink间隔多长将临时文件滚动成最终目标文件
    rollInterval = context.getLong("hdfs.rollInterval", defaultRollInterval);//30
    //以文件大小触发文件滚动，单位字节(0 = 不滚动)
    rollSize = context.getLong("hdfs.rollSize", defaultRollSize);//1024
    //以写入的事件数触发文件滚动， 0=不滚动
    rollCount = context.getLong("hdfs.rollCount", defaultRollCount);//10
    //批次数，HDFS Sink每次从Channel中拿的事件个数。默认值100
    batchSize = context.getLong("hdfs.batchSize", defaultBatchSize);//100
    //超时多久以后关闭无效的文件。(0 = 禁用自动关闭的空闲文件)
    //但是还是可能因为网络等多种原因导致，正在写的文件始终没有关闭，从而产生tmp文件
    idleTimeout = context.getInteger("hdfs.idleTimeout", 0);
    //压缩编解码器，可以使用：gzip, bzip2, lzo, lzop, snappy
    String codecName = context.getString("hdfs.codeC");
    //文件格式：通常使用SequenceFile（默认）, DataStream 或者 CompressedStrea
    //(1)DataStream不能压缩输出文件，不需要设置hdfs.codeC。
    //(2)CompressedStream要求设置hdfs.codeC来制定一个有效的编码解码器。
    fileType = context.getString("hdfs.fileType", defaultFileType);
    //HDFS中允许打开文件的数据，如果数量超过了，最早的文件将被关闭。
    maxOpenFiles = context.getInteger("hdfs.maxOpenFiles", defaultMaxOpenFiles);//5000
    //允许HDFS操作的毫秒数，例如：open，write, flush, close。如果很多HFDS操作超时，这个配置应该增大。
    callTimeout = context.getLong("hdfs.callTimeout", defaultCallTimeout);//10000
    //每个HDFS sink的HDFS的IO操作线程数（例如：open，write）
    threadsPoolSize = context.getInteger("hdfs.threadsPoolSize",
        defaultThreadPoolSize);//10
    //每个HDFS sink调度定时文件滚动的线程数。
    rollTimerPoolSize = context.getInteger("hdfs.rollTimerPoolSize",
        defaultRollTimerPoolSize);//1
    //HDFS安全认证kerberos配置 hadoop.security.authentication属性 有simple 和kerberos 等方式
    String kerbConfPrincipal = context.getString("hdfs.kerberosPrincipal");
    String kerbKeytab = context.getString("hdfs.kerberosKeytab");
    //代理用户
    String proxyUser = context.getString("hdfs.proxyUser");
    tryCount = context.getInteger("hdfs.closeTries", defaultTryCount);
    if (tryCount <= 0) {
      LOG.warn("Retry count value : " + tryCount + " is not " +
          "valid. The sink will try to close the file until the file " +
          "is eventually closed.");
      tryCount = defaultTryCount;
    }
    //关闭HDFS文件失败后重新尝试关闭的延迟数，单位是秒
    retryInterval = context.getLong("hdfs.retryInterval", defaultRetryInterval);//180
    if (retryInterval <= 0) {
      LOG.warn("Retry Interval value: " + retryInterval + " is not " +
          "valid. If the first close of a file fails, " +
          "it may remain open and will not be renamed.");
      tryCount = 1;
    }

    Preconditions.checkArgument(batchSize > 0, "batchSize must be greater than 0");
    //如果hdfs.codeC没有设置
    if (codecName == null) {
      //不压缩数据
      codeC = null;
      compType = CompressionType.NONE;
    } else {
      //调用getCodec方法获取压缩格式
      codeC = getCodec(codecName);
      // TODO : set proper compression type
      //压缩类型为BLOCK类型
      compType = CompressionType.BLOCK;
    }

    // Do not allow user to set fileType DataStream with codeC together
    // To prevent output file with compress extension (like .snappy)
    //如果fileType是DataStream，则不允许压缩
    if (fileType.equalsIgnoreCase(HDFSWriterFactory.DataStreamType) && codecName != null) {
      throw new IllegalArgumentException("fileType: " + fileType +
          " which does NOT support compressed output. Please don't set codeC" +
          " or change the fileType if compressed output is desired.");
    }

    //如果fileType是压缩类型，则codeC不允许为空
    if (fileType.equalsIgnoreCase(HDFSWriterFactory.CompStreamType)) {
      Preconditions.checkNotNull(codeC, "It's essential to set compress codec"
          + " when fileType is: " + fileType);
    }

    // get the appropriate executor
    this.privExecutor = FlumeAuthenticationUtil.getAuthenticator(
            kerbConfPrincipal, kerbKeytab).proxyAs(proxyUser);

    //hdfs.round 是否启用时间上的”舍弃”，这里的”舍弃”，类似于”四舍五入”
    //时间戳是否四舍五入（如果为true，会影响所有基于时间的转义序列 ％t除外）
    needRounding = context.getBoolean("hdfs.round", false);

    if (needRounding) {
      //时间上进行”四舍五入”的单位，包含：second,minute,hour /s
      String unit = context.getString("hdfs.roundUnit", "second");
      if (unit.equalsIgnoreCase("hour")) {
        this.roundUnit = Calendar.HOUR_OF_DAY;
      } else if (unit.equalsIgnoreCase("minute")) {
        this.roundUnit = Calendar.MINUTE;
      } else if (unit.equalsIgnoreCase("second")) {
        this.roundUnit = Calendar.SECOND;
      } else {
        LOG.warn("Rounding unit is not valid, please set one of" +
            "minute, hour, or second. Rounding will be disabled");
        needRounding = false;
      }
      //时间上进行“四舍五入”的值；
      this.roundValue = context.getInteger("hdfs.roundValue", 1);
      //检查是否符合分、秒数值,0<v<=60
      if (roundUnit == Calendar.SECOND || roundUnit == Calendar.MINUTE) {
        Preconditions.checkArgument(roundValue > 0 && roundValue <= 60,
            "Round value" +
            "must be > 0 and <= 60");
      } else if (roundUnit == Calendar.HOUR_OF_DAY) {
        //检查是否符合时数值0<v<=24
        Preconditions.checkArgument(roundValue > 0 && roundValue <= 24,
            "Round value" +
            "must be > 0 and <= 24");
      }
    }

    //hdfs.useLocalTimeStamp 是否使用当地时间,这个属性的目的就是相当于时间戳的拦截器
    this.useLocalTime = context.getBoolean("hdfs.useLocalTimeStamp", false);
    if (useLocalTime) {
      clock = new SystemClock();
    }

    //构造计数器
    if (sinkCounter == null) {
      sinkCounter = new SinkCounter(getName());
    }
  }

  private static boolean codecMatches(Class<? extends CompressionCodec> cls, String codecName) {
    String simpleName = cls.getSimpleName();
    if (cls.getName().equals(codecName) || simpleName.equalsIgnoreCase(codecName)) {
      return true;
    }
    if (simpleName.endsWith("Codec")) {
      String prefix = simpleName.substring(0, simpleName.length() - "Codec".length());
      if (prefix.equalsIgnoreCase(codecName)) {
        return true;
      }
    }
    return false;
  }

  @VisibleForTesting
  static CompressionCodec getCodec(String codecName) {
    Configuration conf = new Configuration();
    List<Class<? extends CompressionCodec>> codecs = CompressionCodecFactory.getCodecClasses(conf);
    // Wish we could base this on DefaultCodec but appears not all codec's
    // extend DefaultCodec(Lzo)
    CompressionCodec codec = null;
    ArrayList<String> codecStrs = new ArrayList<String>();
    codecStrs.add("None");
    for (Class<? extends CompressionCodec> cls : codecs) {
      codecStrs.add(cls.getSimpleName());
      if (codecMatches(cls, codecName)) {
        try {
          codec = cls.newInstance();
        } catch (InstantiationException e) {
          LOG.error("Unable to instantiate " + cls + " class");
        } catch (IllegalAccessException e) {
          LOG.error("Unable to access " + cls + " class");
        }
      }
    }

    if (codec == null) {
      if (!codecName.equalsIgnoreCase("None")) {
        throw new IllegalArgumentException("Unsupported compression codec "
            + codecName + ".  Please choose from: " + codecStrs);
      }
    } else if (codec instanceof org.apache.hadoop.conf.Configurable) {
      // Must check instanceof codec as BZip2Codec doesn't inherit Configurable
      // Must set the configuration for Configurable objects that may or do use
      // native libs
      ((org.apache.hadoop.conf.Configurable) codec).setConf(conf);
    }
    return codec;
  }


  /**
   * Pull events out of channel and send it to HDFS. Take at most batchSize
   * events per Transaction. Find the corresponding bucket for the event.
   * Ensure the file is open. Serialize the data and write it to the file on
   * HDFS. <br/>
   * This method is not thread safe.
   * 从channel中pull出数据并发送到hdfs中（每一个transaction中最多可以有batchSize条Event），
   * 序列化数据并写入hdfs文件
   */
  public Status process() throws EventDeliveryException {
    // 获取Channel
    Channel channel = getChannel();
    //获取事物
    Transaction transaction = channel.getTransaction();
    // Channel的事务启动
    transaction.begin();
    try {
      // 构造一个BucketWriter集合，BucketWriter就是处理hdfs文件的具体逻辑实现类
      //List<BucketWriter> writers = Lists.newArrayList();??
      Set<BucketWriter> writers = new LinkedHashSet<>();
      int txnEventCount = 0;
      //批量处理 batchSize个事件。这里的batchSize就是之前配置的hdfs.batchSize
      for (txnEventCount = 0; txnEventCount < batchSize; txnEventCount++) {
        //如果event==null，说明channel已无数据，则退出循环
        Event event = channel.take();
        if (event == null) {
          break;
        }

        // reconstruct the path name by substituting place holders
        // 构造hdfs文件所在的路径
        /**escapeString这个会将目录和文件名前缀进行格式化，
         * escapeString用于替换%{yyy}的设置和%x的设置，需要设置为%x或者%{yyy}的形式,yyy可以是单词字符,和.或者-其调用replaceShorthand
         */
        String realPath = BucketPath.escapeString(filePath, event.getHeaders(),
            timeZone, needRounding, roundUnit, roundValue, useLocalTime);
        // 构造hdfs文件名
        String realName = BucketPath.escapeString(fileName, event.getHeaders(),
            timeZone, needRounding, roundUnit, roundValue, useLocalTime);

        //1.构造hdfs文件路径，根据之前的path，filePrefix，fileSuffix
        String lookupPath = realPath + DIRECTORY_DELIMITER + realName;
        //2.声明一个BucketWriter对象和HDFSWriter 对象
        //BucketWriter可以理解成对hdfs文件和写入方法的封装
        BucketWriter bucketWriter;
        //HDFSWriter由hdfs.fileType设定，负责实际数据的写入
        HDFSWriter hdfsWriter = null;
        // Callback to remove the reference to the bucket writer from the
        // sfWriters map so that all buffers used by the HDFS file
        // handles are garbage collected.
        // 构造一个回调函数
        WriterCallback closeCallback = new WriterCallback() {
          @Override
          public void run(String bucketPath) {
            LOG.info("Writer callback called.");
            synchronized (sfWritersLock) {
              /** sfWriters是一个HashMap，最多支持maxOpenFiles个键值对。超过maxOpenFiles的话会关闭越早进来的文件
               回调函数的作用就是hdfs文件close的时候移除sfWriters中对应的那个文件。防止打开的文件数超过maxOpenFiles
               sfWriters这个Map中的key是要写的hdfs路径，value是BucketWriter */
              sfWriters.remove(bucketPath);
            }
          }
        };
        synchronized (sfWritersLock) {
          //根据HDFS的绝对路径获取对应的BucketWriter对象,每个lookupPath对应一个BucketWriter对象，对应关系写入到sfWriters
          bucketWriter = sfWriters.get(lookupPath);
          // we haven't seen this file yet, so open it and cache the handle
          if (bucketWriter == null) {
            /**
             * 没有的话构造一个BucketWriter
             * HDFSWriterFactory工厂类会根据配置文件中设置的类型返回相应的HDFSWriter对象，
             * 没有配置文件类型的话默认是HDFSSequenceFile
             * fileType默认有3种类型，分别是SequenceFile, DataStream or CompressedStream
             */
            hdfsWriter = writerFactory.getWriter(fileType);
            // 构造一个BucketWriter，会将刚刚构造的hdfsWriter当做参数传入，BucketWriter写hdfs文件的时候会使用HDFSWriter
            bucketWriter = initializeBucketWriter(realPath, realName,
              lookupPath, hdfsWriter, closeCallback);
            // 新构造的BucketWriter放入到sfWriters中
            sfWriters.put(lookupPath, bucketWriter);
          }
        }

        // Write the data to HDFS
        // 写hdfs数据
        try {
          //3.将event写入bucketWriter对应的文件中
          bucketWriter.append(event);
        } catch (BucketClosedException ex) {
          LOG.info("Bucket was closed while trying to append, " +
                   "reinitializing bucket and writing event.");
          hdfsWriter = writerFactory.getWriter(fileType);
          bucketWriter = initializeBucketWriter(realPath, realName,
            lookupPath, hdfsWriter, closeCallback);
          synchronized (sfWritersLock) {
            sfWriters.put(lookupPath, bucketWriter);
          }
          bucketWriter.append(event);
        }

        // track the buckets getting written in this transaction
        //如果BucketWriter列表没有正在写的文件——bucketWriter
        // 将BucketWriter放入到writers集合中
        if (!writers.contains(bucketWriter)) {
          writers.add(bucketWriter);
        }
      }

      //如果没有处理任何event
      if (txnEventCount == 0) {
        sinkCounter.incrementBatchEmptyCount();
      } else if (txnEventCount == batchSize) {  //一次处理batchSize个event
        sinkCounter.incrementBatchCompleteCount();
      } else { //channel中剩余的events不足batchSize
        sinkCounter.incrementBatchUnderflowCount();
      }

      // flush all pending buckets before committing the transaction
      // 每个批次全部完成后flush所有的hdfs文件
      for (BucketWriter bucketWriter : writers) {
        bucketWriter.flush();
      }
      // 事务提交
      transaction.commit();

      if (txnEventCount < 1) {
        return Status.BACKOFF;
      } else {
        sinkCounter.addToEventDrainSuccessCount(txnEventCount);
        //process方法最后返回的是代表Sink状态的Status对象（BACKOFF或者READY），这个可以用于判断Sink的健康状态，
        // 比如failover的SinkProcessor就根据这个来判断Sink是否可以提供服务
        return Status.READY;
      }
    } catch (IOException eIO) {
      // 发生异常事务回滚
      transaction.rollback();
      LOG.warn("HDFS IO error", eIO);
      return Status.BACKOFF;
    } catch (Throwable th) {
      transaction.rollback();
      LOG.error("process failed", th);
      if (th instanceof Error) {
        throw (Error) th;
      } else {
        throw new EventDeliveryException(th);
      }
    } finally {
      // 关闭事务
      transaction.close();
    }
  }

  @VisibleForTesting
  BucketWriter initializeBucketWriter(String realPath,
      String realName, String lookupPath, HDFSWriter hdfsWriter,
      WriterCallback closeCallback) {
    BucketWriter bucketWriter = new BucketWriter(rollInterval,
        rollSize, rollCount,
        batchSize, context, realPath, realName, inUsePrefix, inUseSuffix,
        suffix, codeC, compType, hdfsWriter, timedRollerPool,
        privExecutor, sinkCounter, idleTimeout, closeCallback,
        lookupPath, callTimeout, callTimeoutPool, retryInterval,
        tryCount);
    if (mockFs != null) {
      bucketWriter.setFileSystem(mockFs);
      bucketWriter.setMockStream(mockWriter);
    }
    return bucketWriter;
  }

  @Override
  public void stop() {
    // do not constrain close() calls with a timeout
    //获取对象锁
    synchronized (sfWritersLock) {
      //遍历对象锁
      for (Entry<String, BucketWriter> entry : sfWriters.entrySet()) {
        LOG.info("Closing {}", entry.getKey());

        //关闭BucketWriter，flush到HDFS
        try {
          entry.getValue().close();
        } catch (Exception ex) {
          LOG.warn("Exception while closing " + entry.getKey() + ". " +
                  "Exception follows.", ex);
          if (ex instanceof InterruptedException) {
            Thread.currentThread().interrupt();
          }
        }
      }
    }

    // shut down all our thread pools
    ExecutorService[] toShutdown = { callTimeoutPool, timedRollerPool };
    for (ExecutorService execService : toShutdown) {
      execService.shutdown();
      try {
        while (execService.isTerminated() == false) {
          execService.awaitTermination(
                  Math.max(defaultCallTimeout, callTimeout), TimeUnit.MILLISECONDS);
        }
      } catch (InterruptedException ex) {
        LOG.warn("shutdown interrupted on " + execService, ex);
      }
    }

    callTimeoutPool = null;
    timedRollerPool = null;

    synchronized (sfWritersLock) {
      sfWriters.clear();
      sfWriters = null;
    }
    sinkCounter.stop();
    super.stop();
  }

  //start()主要是初始化两个线程池
  @Override
  public void start() {
    String timeoutName = "hdfs-" + getName() + "-call-runner-%d";
    //线程池用于event写入HDFS文件
    /**创建一个可重用固定线程数的线程池，以共享的无界队列方式来运行这些线程，
     * 在需要时使用提供的 ThreadFactory 创建新线程。在任意点，在大多数 nThreads 线程会处于处理任务的活动状态。
     * 如果在所有线程处于活动状态时提交附加任务，则在有可用线程之前，附加任务将在队列中等待。
     * 如果在关闭前的执行期间由于失败而导致任何线程终止，那么一个新线程将代替它执行后续的任务（如果需要）。
     * 在某个线程被显式地关闭之前，池中的线程将一直存在。
     参数：nThreads - 池中的线程数
          threadFactory - 创建新线程时使用的工厂
     返回：新创建的线程池
     抛出：NullPointerException - 如果 threadFactory 为 null
          IllegalArgumentException - 如果 nThreads <= 0
     */
    callTimeoutPool = Executors.newFixedThreadPool(threadsPoolSize,
            new ThreadFactoryBuilder().setNameFormat(timeoutName).build());

    String rollerName = "hdfs-" + getName() + "-roll-timer-%d";
    //该线程池用来滚动文件
    /**
     *  创建一个线程池，它可安排在给定延迟后运行命令或者定期地执行。
        参数：corePoolSize - 池中所保存的线程数，即使线程是空闲的也包括在内
             threadFactory - 执行程序创建新线程时使用的工厂
        返回：新创建的安排线程池
        抛出：IllegalArgumentException - 如果 corePoolSize < 0
             NullPointerException - 如果 threadFactory 为 null
     */
    //通过callWithTimeout方法调用，并实现timeout功能
    timedRollerPool = Executors.newScheduledThreadPool(rollTimerPoolSize,
            new ThreadFactoryBuilder().setNameFormat(rollerName).build());

    //该LinkedHashMap用来存储文件的绝对路径以及对应的BucketWriter
    this.sfWriters = new WriterLinkedHashMap(maxOpenFiles);
    sinkCounter.start();
    super.start();
  }

  @Override
  public String toString() {
    return "{ Sink type:" + getClass().getSimpleName() + ", name:" + getName() +
            " }";
  }

  @VisibleForTesting
  void setBucketClock(Clock clock) {
    BucketPath.setClock(clock);
  }

  @VisibleForTesting
  void setMockFs(FileSystem mockFs) {
    this.mockFs = mockFs;
  }

  @VisibleForTesting
  void setMockWriter(HDFSWriter writer) {
    this.mockWriter = writer;
  }

  @VisibleForTesting
  int getTryCount() {
    return tryCount;
  }
}
