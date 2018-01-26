/*
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.instrumentation;

import org.apache.commons.lang.ArrayUtils;

/**Flume-NG内置计数器(监控)
 *就是各个组件的统计信息，比如成功接收的Event数量、成功发送的Event数量，处理的Transaction的数量等等。
 * 而且不同的组件有不同的Countor来做统计，目前直到1.5版本仍然只对三大组件：source、sink、channel
 * 进行统计分别是SourceCounter、SinkCounter、ChannelCounter，
 * 这三个计数器的统计项是固定的，就是你不能自己设置自己的统计项
 * (有些内置的组件使用CounterGroup这个来统计信息，这个可以自己随意设置统计项，
 *  但是目前这个可以自定义的计数器的信息还无法用在监控上，因为这只是一个单独的类，并没有继承MonitoredCounterGroup这个抽象类)
 *  SinkCounter各种统计项
 　　(1)"sink.connection.creation.count"，表示“链接”创建的数量，比如与HBase建立链接，与avrosource建立链接以及文件的打开等；
 　　(2)"sink.connection.closed.count"，对应于上面的stop操作、destroyConnection、close文件操作等。
 　　(3)"sink.connection.failed.count"，表示上面所表示“链接”时异常、失败的次数；
 　　(4)"sink.batch.empty"，表示这个批次处理的event数量为0的情况；
 　　(5)"sink.batch.underflow"，表示这个批次处理的event的数量介于0和配置的batchSize之间；
 　　(6)"sink.batch.complete"，表示这个批次处理的event数量等于设定的batchSize；
 　　(7)"sink.event.drain.attempt"，准备处理的event的个数；
 　　(8)"sink.event.drain.sucess"，这个表示处理成功的event数量，与上面不同的是上面的是还未处理的。
 */
public class SinkCounter extends MonitoredCounterGroup implements
    SinkCounterMBean {

  private static final String COUNTER_CONNECTION_CREATED =
      "sink.connection.creation.count";

  private static final String COUNTER_CONNECTION_CLOSED =
      "sink.connection.closed.count";

  private static final String COUNTER_CONNECTION_FAILED =
      "sink.connection.failed.count";

  private static final String COUNTER_BATCH_EMPTY =
      "sink.batch.empty";

  private static final String COUNTER_BATCH_UNDERFLOW =
      "sink.batch.underflow";

  private static final String COUNTER_BATCH_COMPLETE =
      "sink.batch.complete";

  private static final String COUNTER_EVENT_DRAIN_ATTEMPT =
      "sink.event.drain.attempt";

  private static final String COUNTER_EVENT_DRAIN_SUCCESS =
      "sink.event.drain.sucess";

  private static final String[] ATTRIBUTES = {
    COUNTER_CONNECTION_CREATED, COUNTER_CONNECTION_CLOSED,
    COUNTER_CONNECTION_FAILED, COUNTER_BATCH_EMPTY,
    COUNTER_BATCH_UNDERFLOW, COUNTER_BATCH_COMPLETE,
    COUNTER_EVENT_DRAIN_ATTEMPT, COUNTER_EVENT_DRAIN_SUCCESS
  };

  public SinkCounter(String name) {
    super(MonitoredCounterGroup.Type.SINK, name, ATTRIBUTES);
  }

  public SinkCounter(String name, String[] attributes) {
    super(MonitoredCounterGroup.Type.SINK, name,
        (String[]) ArrayUtils.addAll(attributes,ATTRIBUTES));
  }

  @Override
  public long getConnectionCreatedCount() {
    return get(COUNTER_CONNECTION_CREATED);
  }

  public long incrementConnectionCreatedCount() {
    return increment(COUNTER_CONNECTION_CREATED);
  }

  @Override
  public long getConnectionClosedCount() {
    return get(COUNTER_CONNECTION_CLOSED);
  }

  public long incrementConnectionClosedCount() {
    return increment(COUNTER_CONNECTION_CLOSED);
  }

  @Override
  public long getConnectionFailedCount() {
    return get(COUNTER_CONNECTION_FAILED);
  }

  public long incrementConnectionFailedCount() {
    return increment(COUNTER_CONNECTION_FAILED);
  }

  @Override
  public long getBatchEmptyCount() {
    return get(COUNTER_BATCH_EMPTY);
  }

  public long incrementBatchEmptyCount() {
    return increment(COUNTER_BATCH_EMPTY);
  }

  @Override
  public long getBatchUnderflowCount() {
    return get(COUNTER_BATCH_UNDERFLOW);
  }

  public long incrementBatchUnderflowCount() {
    return increment(COUNTER_BATCH_UNDERFLOW);
  }

  @Override
  public long getBatchCompleteCount() {
    return get(COUNTER_BATCH_COMPLETE);
  }

  public long incrementBatchCompleteCount() {
    return increment(COUNTER_BATCH_COMPLETE);
  }

  @Override
  public long getEventDrainAttemptCount() {
    return get(COUNTER_EVENT_DRAIN_ATTEMPT);
  }

  public long incrementEventDrainAttemptCount() {
    return increment(COUNTER_EVENT_DRAIN_ATTEMPT);
  }

  public long addToEventDrainAttemptCount(long delta) {
    return addAndGet(COUNTER_EVENT_DRAIN_ATTEMPT, delta);
  }

  @Override
  public long getEventDrainSuccessCount() {
    return get(COUNTER_EVENT_DRAIN_SUCCESS);
  }

  public long incrementEventDrainSuccessCount() {
    return increment(COUNTER_EVENT_DRAIN_SUCCESS);
  }

  public long addToEventDrainSuccessCount(long delta) {
    return addAndGet(COUNTER_EVENT_DRAIN_SUCCESS, delta);
  }
}
