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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import org.apache.flume.Clock;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.SystemClock;
import org.apache.flume.auth.PrivilegedExecutor;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.hdfs.HDFSEventSink.WriterCallback;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Internal API intended for HDFSSink use.
 * This class does file rolling and handles file formats and serialization.
 * Only the public methods in this class are thread safe.
 */
class BucketWriter {

  private static final Logger LOG = LoggerFactory
      .getLogger(BucketWriter.class);

  /**
   * This lock ensures that only one thread can open a file at a time.
   */
  private static final Integer staticLock = new Integer(1);
  private Method isClosedMethod = null;

  private HDFSWriter writer;
  private final long rollInterval;
  private final long rollSize;
  private final long rollCount;
  private final long batchSize;
  private final CompressionCodec codeC;
  private final CompressionType compType;
  private final ScheduledExecutorService timedRollerPool;
  private final PrivilegedExecutor proxyUser;

  private final AtomicLong fileExtensionCounter;

  private long eventCounter;
  private long processSize;

  private FileSystem fileSystem;

  private volatile String filePath;
  private volatile String fileName;
  private volatile String inUsePrefix;
  private volatile String inUseSuffix;
  private volatile String fileSuffix;
  private volatile String bucketPath;
  private volatile String targetPath;
  private volatile long batchCounter;
  private volatile boolean isOpen;
  private volatile boolean isUnderReplicated;
  private volatile int consecutiveUnderReplRotateCount = 0;
  private volatile ScheduledFuture<Void> timedRollFuture;
  private SinkCounter sinkCounter;
  private final int idleTimeout;
  /**java.util.concurrent interface ScheduledFuture<V> extends Delayed, Future<V>
   * 接口 Delayed 一种混合风格的接口，用来标记那些应该在给定延迟时间之后执行的对象。
   *     此接口的实现必须定义一个 compareTo 方法，该方法提供与此接口的 getDelay 方法一致的排序。
   * 接口 Future 是与Runnable,Callable进行交互的接口，比如一个线程执行结束后取返回的结果等等，还提供了cancel终止线程。
   *     它提供了检查计算是否完成的方法，以等待计算的完成，并获取计算的结果。
   *     取消则由 cancel 方法来执行。还提供了其他方法，以确定任务是正常完成还是被取消了。
   *     一旦计算完成，就不能再取消计算。
   */
  private volatile ScheduledFuture<Void> idleFuture;
  private final WriterCallback onCloseCallback;
  private final String onCloseCallbackPath;
  private final long callTimeout;
  private final ExecutorService callTimeoutPool;
  /**
   * append()读取了一个固定变量maxConsecUnderReplRotations=30，
   * 也就是正在复制的块，最多之能滚动出30个文件，如果超过了30次，该数据块如果还在复制中，那么数据也不会滚动了，
   * doRotate=false，所以有的人发现自己一旦运行一段时间，会出现30个文件
   */
  private final int maxConsecUnderReplRotations = 30; // make this config'able?

  private boolean mockFsInjected = false;

  private final long retryInterval;
  private final int maxRenameTries;

  // flag that the bucket writer was closed due to idling and thus shouldn't be
  // reopened. Not ideal, but avoids internals of owners
  protected boolean closed = false;
  AtomicInteger renameTries = new AtomicInteger(0);

  BucketWriter(long rollInterval, long rollSize, long rollCount, long batchSize,
      Context context, String filePath, String fileName, String inUsePrefix,
      String inUseSuffix, String fileSuffix, CompressionCodec codeC,
      CompressionType compType, HDFSWriter writer,
      ScheduledExecutorService timedRollerPool, PrivilegedExecutor proxyUser,
      SinkCounter sinkCounter, int idleTimeout, WriterCallback onCloseCallback,
      String onCloseCallbackPath, long callTimeout,
      ExecutorService callTimeoutPool, long retryInterval,
      int maxCloseTries) {
    this(rollInterval, rollSize, rollCount, batchSize,
            context, filePath, fileName, inUsePrefix,
            inUseSuffix, fileSuffix, codeC,
            compType, writer,
            timedRollerPool, proxyUser,
            sinkCounter, idleTimeout, onCloseCallback,
            onCloseCallbackPath, callTimeout,
            callTimeoutPool, retryInterval,
            maxCloseTries, new SystemClock());
  }

  //BucketWriter的构造函数首先是对众多参数赋值，然后isOpen = false，
  // 最后this.writer.configure(context)，即对writer对象进行配置
  BucketWriter(long rollInterval, long rollSize, long rollCount, long batchSize,
           Context context, String filePath, String fileName, String inUsePrefix,
           String inUseSuffix, String fileSuffix, CompressionCodec codeC,
           CompressionType compType, HDFSWriter writer,
           ScheduledExecutorService timedRollerPool, PrivilegedExecutor proxyUser,
           SinkCounter sinkCounter, int idleTimeout, WriterCallback onCloseCallback,
           String onCloseCallbackPath, long callTimeout,
           ExecutorService callTimeoutPool, long retryInterval,
           int maxCloseTries, Clock clock) {
    this.rollInterval = rollInterval;
    this.rollSize = rollSize;
    this.rollCount = rollCount;
    this.batchSize = batchSize;
    this.filePath = filePath;
    this.fileName = fileName;
    this.inUsePrefix = inUsePrefix;
    this.inUseSuffix = inUseSuffix;
    this.fileSuffix = fileSuffix;
    this.codeC = codeC;
    this.compType = compType;
    this.writer = writer;
    this.timedRollerPool = timedRollerPool;
    this.proxyUser = proxyUser;
    this.sinkCounter = sinkCounter;
    this.idleTimeout = idleTimeout;
    this.onCloseCallback = onCloseCallback;
    this.onCloseCallbackPath = onCloseCallbackPath;
    this.callTimeout = callTimeout;
    this.callTimeoutPool = callTimeoutPool;
    fileExtensionCounter = new AtomicLong(clock.currentTimeMillis());

    this.retryInterval = retryInterval;
    this.maxRenameTries = maxCloseTries;
    isOpen = false;
    isUnderReplicated = false;
    this.writer.configure(context);
  }

  @VisibleForTesting
  void setFileSystem(FileSystem fs) {
    this.fileSystem = fs;
    mockFsInjected = true;
  }

  @VisibleForTesting
  void setMockStream(HDFSWriter dataWriter) {
    this.writer = dataWriter;
  }


  /**
   * Clear the class counters
   */
  private void resetCounters() {
    eventCounter = 0;
    processSize = 0;
    batchCounter = 0;
  }

  private Method getRefIsClosed() {
    try {
      return fileSystem.getClass().getMethod("isFileClosed",
        Path.class);
    } catch (Exception e) {
      LOG.info("isFileClosed() is not available in the version of the " +
               "distributed filesystem being used. " +
               "Flume will not attempt to re-close files if the close fails " +
               "on the first attempt");
      return null;
    }
  }

  private Boolean isFileClosed(FileSystem fs, Path tmpFilePath) throws Exception {
    return (Boolean)(isClosedMethod.invoke(fs, tmpFilePath));
  }

  /**
   * open() is called by append()
   * @throws IOException
   * @throws InterruptedException
   * 打开hdfs文件的方法
   */
  private void open() throws IOException, InterruptedException {
    // hdfs文件路径或HDFSWriter null的话抛出异常
    if ((filePath == null) || (writer == null)) {
      throw new IOException("Invalid file settings");
    }

    final Configuration config = new Configuration();
    // disable FileSystem JVM shutdown hook
    //hadoop 属性 fs.automatic.close=true
    //文件系统实例在程序退出时自动关闭，通过JVM shutdown hook方式。
    // 可以把此属性设置为false取消这种操作。这是一个高级选项，需要使用者特别关注关闭顺序。不要关闭
    config.setBoolean("fs.automatic.close", false);

    // Hadoop is not thread safe when doing certain RPC operations,
    // including getFileSystem(), when running under Kerberos.
    // open() must be called by one thread at a time in the JVM.
    // NOTE: tried synchronizing on the underlying Kerberos principal previously
    // which caused deadlocks. See FLUME-1231.
    synchronized (staticLock) {
      //首先会检查当前线程是否中断
      checkAndThrowInterruptedException();

      try {
        //fileExtensionCounter是一个AtomicLong类型的实例，初始化为当前时间戳的数值
        long counter = fileExtensionCounter.incrementAndGet();
        //最终的文件名加上时间戳，这就是为什么flume生成的文件名会带有时间戳的原因 eg flume.1437375933234
        String fullFileName = fileName + "." + counter;

        // 加上后缀名， fullFileName就成了flume.1437375933234.txt
        //后缀名和压缩不能同时兼得，即如果没有配置压缩则可以在fullFileName后面添加自定义的后缀(比如后缀为.txt)，
        // 否则只能添加压缩类型的后缀
        if (fileSuffix != null && fileSuffix.length() > 0) {
          fullFileName += fileSuffix;
        } else if (codeC != null) {
          fullFileName += codeC.getDefaultExtension();
        }
        // 由于没配置inUsePrefix和inUseSuffix。 故这两个属性的值分别为""和".tmp"
        // buckerPath为 /data/2020/07/20/15/flume.1437375933234.txt.tmp
        bucketPath = filePath + "/" + inUsePrefix
          + fullFileName + inUseSuffix;
        // targetPath为 /data/2020/07/20/15/flume.1437375933234.txt
        targetPath = filePath + "/" + fullFileName;

        LOG.info("Creating " + bucketPath);
        //提供了调用的超时功能
        //这个方法会将对应的Callable放入callTimeoutPool线程池中执行，并等待callTimeout(默认是10000) ms返回结果
        callWithTimeout(new CallRunner<Void>() {
          @Override
          public Void call() throws Exception {
            //根据有无压缩配置信息open此witer，
            // 没有压缩：writer.open(bucketPath)，
            // 有压缩：writer.open(bucketPath, codeC, compType)
            if (codeC == null) {
              // Need to get reference to FS using above config before underlying
              // writer does in order to avoid shutdown hook &
              // IllegalStateExceptions
              if (!mockFsInjected) {
                fileSystem = new Path(bucketPath).getFileSystem(config);
              }
              // 使用HDFSWriter打开文件
              writer.open(bucketPath);
            } else {
              // need to get reference to FS before writer does to
              // avoid shutdown hook
              if (!mockFsInjected) {
                fileSystem = new Path(bucketPath).getFileSystem(config);
              }
              writer.open(bucketPath, codeC, compType);
            }
            return null;
          }
        });
      } catch (Exception ex) {
        sinkCounter.incrementConnectionFailedCount();
        if (ex instanceof IOException) {
          throw (IOException) ex;
        } else {
          throw Throwables.propagate(ex);
        }
      }
    }
    isClosedMethod = getRefIsClosed();
    sinkCounter.incrementConnectionCreatedCount();
    // 重置各个计数器
    resetCounters();

    // if time-based rolling is enabled, schedule the roll
    // 开线程处理hdfs.rollInterval配置的参数，多长时间后调用close方法
    if (rollInterval > 0) {
      //如果rollInterval(按时间滚动文件)不为0，则创建一个Callable，
      // 放入timedRollFuture中rollInterval秒之后关闭文件，默认是30s写一个文件，这是控制文件滚动的3个条件之一
      Callable<Void> action = new Callable<Void>() {
        public Void call() throws Exception {
          LOG.debug("Rolling file ({}): Roll scheduled after {} sec elapsed.",
              bucketPath, rollInterval);
          try {
            // Roll the file and remove reference from sfWriters map.
            close(true);
          } catch (Throwable t) {
            LOG.error("Unexpected error", t);
          }
          return null;
        }
      };
      // 以秒为单位在这里指定。将这个线程执行的结果赋值给timedRollFuture这个属性
      timedRollFuture = timedRollerPool.schedule(action, rollInterval,
          TimeUnit.SECONDS);
    }

    isOpen = true;
  }

  /**
   * Close the file handle and rename the temp file to the permanent filename.
   * Safe to call multiple times. Logs HDFSWriter.close() exceptions. This
   * method will not cause the bucket writer to be dereferenced from the HDFS
   * sink that owns it. This method should be used only when size or count
   * based rolling closes this file.
   * @throws IOException On failure to rename if temp file exists.
   * @throws InterruptedException
   */
  public synchronized void close() throws IOException, InterruptedException {
    close(false);
  }

  private CallRunner<Void> createCloseCallRunner() {
    return new CallRunner<Void>() {
      private final HDFSWriter localWriter = writer;
      @Override
      public Void call() throws Exception {
        localWriter.close(); // could block
        return null;
      }
    };
  }

  private Callable<Void> createScheduledRenameCallable() {

    return new Callable<Void>() {
      private final String path = bucketPath;
      private final String finalPath = targetPath;
      private FileSystem fs = fileSystem;
      private int renameTries = 1; // one attempt is already done

      @Override
      public Void call() throws Exception {
        if (renameTries >= maxRenameTries) {
          LOG.warn("Unsuccessfully attempted to rename " + path + " " +
              maxRenameTries + " times. File may still be open.");
          return null;
        }
        renameTries++;
        try {
          renameBucket(path, finalPath, fs);
        } catch (Exception e) {
          LOG.warn("Renaming file: " + path + " failed. Will " +
              "retry again in " + retryInterval + " seconds.", e);
          timedRollerPool.schedule(this, retryInterval, TimeUnit.SECONDS);
          return null;
        }
        return null;
      }
    };
  }

  /**
   * Tries to start the lease recovery process for the current bucketPath
   * if the fileSystem is DistributedFileSystem.
   * Catches and logs the IOException.
   */
  private synchronized void recoverLease() {
    if (bucketPath != null && fileSystem instanceof DistributedFileSystem) {
      try {
        LOG.debug("Starting lease recovery for {}", bucketPath);
        ((DistributedFileSystem) fileSystem).recoverLease(new Path(bucketPath));
      } catch (IOException ex) {
        LOG.warn("Lease recovery failed for {}", bucketPath, ex);
      }
    }
  }

  /**
   * Close the file handle and rename the temp file to the permanent filename.
   * Safe to call multiple times. Logs HDFSWriter.close() exceptions.
   * @throws IOException On failure to rename if temp file exists.
   * @throws InterruptedException
   */
  public synchronized void close(boolean callCloseCallback)
      throws IOException, InterruptedException {
    checkAndThrowInterruptedException();
    try {
      // close的时候先执行flush方法，清空batchCount，并调用HDFSWriter的sync方法
      flush();
    } catch (IOException e) {
      LOG.warn("pre-close flush failed", e);
    }

    LOG.info("Closing {}", bucketPath);
    // 创建一个关闭线程，这个线程会调用HDFSWriter的close方法
    CallRunner<Void> closeCallRunner = createCloseCallRunner();
    // 如果文件还开着
    if (isOpen) {
      try {
        // 执行HDFSWriter的close方法
        callWithTimeout(closeCallRunner);
        sinkCounter.incrementConnectionClosedCount();
      } catch (IOException e) {
        LOG.warn("failed to close() HDFSWriter for file (" + bucketPath +
                 "). Exception follows.", e);
        sinkCounter.incrementConnectionFailedCount();
        // starting lease recovery process, see FLUME-3080
        recoverLease();
      }
      isOpen = false;
    } else {
      LOG.info("HDFSWriter is already closed: {}", bucketPath);
    }

    // NOTE: timed rolls go through this codepath as well as other roll types
    /**
     * timedRollFuture就是根据hdfs.rollInterval配置生成的一个属性。如果hdfs.rollInterval配置为0，那么不会执行以下代码
       因为要close文件，所以如果开启了hdfs.rollInterval等待时间到了flush文件，由于文件已经关闭，再次关闭会有问题
       所以这里取消timedRollFuture线程的执行
     */
    if (timedRollFuture != null && !timedRollFuture.isDone()) {
      timedRollFuture.cancel(false); // do not cancel myself if running!
      timedRollFuture = null;
    }

    // 没有配置hdfs.idleTimeout， 不会执行
    if (idleFuture != null && !idleFuture.isDone()) {
      idleFuture.cancel(false); // do not cancel myself if running!
      idleFuture = null;
    }

    // 重命名文件，如果报错了，不会重命名文件
    if (bucketPath != null && fileSystem != null) {
      // could block or throw IOException
      try {
        renameBucket(bucketPath, targetPath, fileSystem);
      } catch (Exception e) {
        LOG.warn("failed to rename() file (" + bucketPath +
                 "). Exception follows.", e);
        sinkCounter.incrementConnectionFailedCount();
        // 关闭文件失败的话起个线程，retryInterval秒后继续执行
        final Callable<Void> scheduledRename = createScheduledRenameCallable();
        timedRollerPool.schedule(scheduledRename, retryInterval, TimeUnit.SECONDS);
      }
    }
    if (callCloseCallback) {// callCloseCallback是close方法的参数
      /**
       *  调用关闭文件的回调函数，也就是BucketWriter的onCloseCallback属性
          这个onCloseCallback属性就是在HDFSEventSink里的回调函数closeCallback。 用来处理sfWriters.remove(bucketPath);
          如果onCloseCallback属性为true，那么说明这个BucketWriter已经不会再次open新的文件了。生命周期已经到了。
          onCloseCallback只有在append方法中调用shouldRotate方法的时候需要close文件的时候才会传入false，其他情况都是true
       */
      runCloseAction();
      closed = true;
    }
  }

  /**
   * flush the data
   * @throws IOException
   * @throws InterruptedException
   * flush方法，只会在close和append方法(处理的事件数等于批次数)中被调用
   */
  public synchronized void flush() throws IOException, InterruptedException {
    checkAndThrowInterruptedException();
    //isBatchComplete判断batchCount是否等于0。 所以这里只要batchCount不为0，那么执行下去
    if (!isBatchComplete()) {
      // doFlush方法会调用HDFSWriter的sync方法，并且将batchCount设置为0
      doFlush();

      // idleTimeout没有配置，以下代码不会执行
      if (idleTimeout > 0) {
        // if the future exists and couldn't be cancelled, that would mean it has already run
        // or been cancelled
        if (idleFuture == null || idleFuture.cancel(false)) {
          Callable<Void> idleAction = new Callable<Void>() {
            public Void call() throws Exception {
              LOG.info("Closing idle bucketWriter {} at {}", bucketPath,
                       System.currentTimeMillis());
              if (isOpen) {
                close(true);
              }
              return null;
            }
          };
          idleFuture = timedRollerPool.schedule(idleAction, idleTimeout,
              TimeUnit.SECONDS);
        }
      }
    }
  }

  private void runCloseAction() {
    try {
      if (onCloseCallback != null) {
        onCloseCallback.run(onCloseCallbackPath);
      }
    } catch (Throwable t) {
      LOG.error("Unexpected error", t);
    }
  }

  /**
   * doFlush() must only be called by flush()
   * @throws IOException
   */
  private void doFlush() throws IOException, InterruptedException {
    callWithTimeout(new CallRunner<Void>() {
      @Override
      public Void call() throws Exception {
        writer.sync(); // could block
        return null;
      }
    });
    batchCounter = 0;
  }

  /**
   * Open file handles, write data, update stats, handle file rolling and
   * batching / flushing. <br />
   * If the write fails, the file is implicitly closed and then the IOException
   * is rethrown. <br />
   * We rotate before append, and not after, so that the active file rolling
   * mechanism will never roll an empty file. This also ensures that the file
   * creation time reflects when the first event was written.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  public synchronized void append(final Event event)
          throws IOException, InterruptedException {
    checkAndThrowInterruptedException();
    // If idleFuture is not null, cancel it before we move forward to avoid a
    // close call in the middle of the append.
    if (idleFuture != null) {
      idleFuture.cancel(false);
      // There is still a small race condition - if the idleFuture is already
      // running, interrupting it can cause HDFS close operation to throw -
      // so we cannot interrupt it while running. If the future could not be
      // cancelled, it is already running - wait for it to finish before
      // attempting to write.
      if (!idleFuture.isDone()) {
        try {
          idleFuture.get(callTimeout, TimeUnit.MILLISECONDS);//给定单元粒度的时间段 毫秒
        } catch (TimeoutException ex) {
          LOG.warn("Timeout while trying to cancel closing of idle file. Idle" +
                   " file close may have failed", ex);
        } catch (Exception ex) {
          LOG.warn("Error while trying to cancel closing of idle file. ", ex);
        }
      }
      idleFuture = null;
    }

    // If the bucket writer was closed due to roll timeout or idle timeout,
    // force a new bucket writer to be created. Roll count and roll size will
    // just reuse this one
    // 1.首先判断文件是否打开 如果hdfs文件没有被打开
    if (!isOpen) {
      // hdfs已关闭的话抛出异常
      if (closed) {
        throw new BucketClosedException("This bucket writer was closed and " +
          "this handle is thus no longer valid");
      }
      // 打开hdfs文件
      open();
    }

    // check if it's time to rotate the file
    // 查看是否需要创建新文件
    if (shouldRotate()) {
      boolean doRotate = true;

      if (isUnderReplicated) {
        if (maxConsecUnderReplRotations > 0 &&
            consecutiveUnderReplRotateCount >= maxConsecUnderReplRotations) {
          doRotate = false;
          if (consecutiveUnderReplRotateCount == maxConsecUnderReplRotations) {
            LOG.error("Hit max consecutive under-replication rotations ({}); " +
                "will not continue rolling files under this path due to " +
                "under-replication", maxConsecUnderReplRotations);
          }
        } else {
          LOG.warn("Block Under-replication detected. Rotating file.");
        }
        consecutiveUnderReplRotateCount++;
      } else {
        consecutiveUnderReplRotateCount = 0;
      }

      if (doRotate) {
        // 如果需要创建新文件的时候会关闭文件，然后再打开新的文件。这里的close方法没有参数，表示可以再次打开新的文件
        close();
        open();
      }
    }

    // write the event
    // 写event数据
    try {
      sinkCounter.incrementEventDrainAttemptCount();
      callWithTimeout(new CallRunner<Void>() {
        @Override
        public Void call() throws Exception {
          // 真正的写数据使用HDFSWriter的append方法
          /** writer有三类
           *1 writer为HDFSSequenceFile：append(event)方法，会先通过serializer.serialize(e)把event处理成一个Key和一个Value。
           *2 writer为HDFSDataStream：append(event)方法直接调用serializer.write(e)。
           *3 writer为HDFSCompressedDataStream：append(event)方法会首先判断是否完成一个阶段的压缩isFinished，
           *  如果是则更新压缩输出流的状态，并isFinished=false，否则剩下的执行和HDFSDataStream.append(event)相同。
           */
          writer.append(event); // could block
          return null;
        }
      });
    } catch (IOException e) {
      LOG.warn("Caught IOException writing to HDFSWriter ({}). Closing file (" +
          bucketPath + ") and rethrowing exception.",
          e.getMessage());
      try {
        close(true);
      } catch (IOException e2) {
        LOG.warn("Caught IOException while closing file (" +
             bucketPath + "). Exception follows.", e2);
      }
      throw e;
    }

    // update statistics
    // 文件大小+起来
    processSize += event.getBody().length;
    // 事件个数+1
    eventCounter++;
    // 批次数+1
    batchCounter++;
    // 批次数达到配置的hdfs.batchSize的话调用flush方法
    if (batchCounter == batchSize) {
      flush();
    }
  }

  /**
   * check if time to rotate the file
   * shouldRotate 判断是否滚动文件 方法执行 会关闭文件然后再次打开新的文件
   * 判断文件中的行数和文件的大小是否达到配置文件中的配置，如果任何一个满足条件则可以关闭文件，
   * 这是控制文件滚动的3个条件中的两个
   */
  private boolean shouldRotate() {
    boolean doRotate = false;

    // 调用HDFSWriter的isUnderReplicated方法，用来判断当前hdfs文件是否正在复制
    if (writer.isUnderReplicated()) {
      this.isUnderReplicated = true;
      doRotate = true;
    } else {
      this.isUnderReplicated = false;
    }

    // rollCount就是配置的hdfs.rollCount。 eventCounter事件数达到rollCount之后，会close文件，然后创建新的文件
    if ((rollCount > 0) && (rollCount <= eventCounter)) {
      LOG.debug("rolling: rollCount: {}, events: {}", rollCount, eventCounter);
      doRotate = true;
    }

    // rollSize就是配置的hdfs.rollSize。processSize是每个事件加起来的文件大小。
    // 当processSize超过rollSize的时候，会close文件，然后创建新的文件
    if ((rollSize > 0) && (rollSize <= processSize)) {
      LOG.debug("rolling: rollSize: {}, bytes: {}", rollSize, processSize);
      doRotate = true;
    }

    return doRotate;
  }

  /**
   * Rename bucketPath file from .tmp to permanent location.
   */
  // When this bucket writer is rolled based on rollCount or
  // rollSize, the same instance is reused for the new file. But if
  // the previous file was not closed/renamed,
  // the bucket writer fields no longer point to it and hence need
  // to be passed in from the thread attempting to close it. Even
  // when the bucket writer is closed due to close timeout,
  // this method can get called from the scheduled thread so the
  // file gets closed later - so an implicit reference to this
  // bucket writer would still be alive in the Callable instance.
  private void renameBucket(String bucketPath, String targetPath, final FileSystem fs)
      throws IOException, InterruptedException {
    if (bucketPath.equals(targetPath)) {
      return;
    }

    final Path srcPath = new Path(bucketPath);
    final Path dstPath = new Path(targetPath);

    callWithTimeout(new CallRunner<Void>() {
      @Override
      public Void call() throws Exception {
        if (fs.exists(srcPath)) { // could block
          LOG.info("Renaming " + srcPath + " to " + dstPath);
          renameTries.incrementAndGet();
          fs.rename(srcPath, dstPath); // could block
        }
        return null;
      }
    });
  }

  @Override
  public String toString() {
    return "[ " + this.getClass().getSimpleName() + " targetPath = " + targetPath +
        ", bucketPath = " + bucketPath + " ]";
  }

  private boolean isBatchComplete() {
    return (batchCounter == 0);
  }

  /**
   * This method if the current thread has been interrupted and throws an
   * exception.
   * @throws InterruptedException
   */
  private static void checkAndThrowInterruptedException()
          throws InterruptedException {
    if (Thread.currentThread().interrupted()) {
      throw new InterruptedException("Timed out before HDFS call was made. "
              + "Your hdfs.callTimeout might be set too low or HDFS calls are "
              + "taking too long.");
    }
  }

  /**
   * Execute the callable on a separate thread and wait for the completion
   * for the specified amount of time in milliseconds. In case of timeout
   * cancel the callable and throw an IOException
   */
  private <T> T callWithTimeout(final CallRunner<T> callRunner)
      throws IOException, InterruptedException {
    Future<T> future = callTimeoutPool.submit(new Callable<T>() {
      @Override
      public T call() throws Exception {
        return proxyUser.execute(new PrivilegedExceptionAction<T>() {
          @Override
          public T run() throws Exception {
            return callRunner.call();
          }
        });
      }
    });
    try {
      if (callTimeout > 0) {
        return future.get(callTimeout, TimeUnit.MILLISECONDS);
      } else {
        return future.get();
      }
    } catch (TimeoutException eT) {
      future.cancel(true);
      sinkCounter.incrementConnectionFailedCount();
      throw new IOException("Callable timed out after " +
        callTimeout + " ms" + " on file: " + bucketPath, eT);
    } catch (ExecutionException e1) {
      sinkCounter.incrementConnectionFailedCount();
      Throwable cause = e1.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      } else if (cause instanceof InterruptedException) {
        throw (InterruptedException) cause;
      } else if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      } else if (cause instanceof Error) {
        throw (Error)cause;
      } else {
        throw new RuntimeException(e1);
      }
    } catch (CancellationException ce) {
      throw new InterruptedException(
        "Blocked callable interrupted by rotation event");
    } catch (InterruptedException ex) {
      LOG.warn("Unexpected Exception " + ex.getMessage(), ex);
      throw ex;
    }
  }

  /**
   * Simple interface whose <tt>call</tt> method is called by
   * {#callWithTimeout} in a new thread inside a
   * {@linkplain java.security.PrivilegedExceptionAction#run()} call.
   * @param <T>
   */
  private interface CallRunner<T> {
    T call() throws Exception;
  }

}
