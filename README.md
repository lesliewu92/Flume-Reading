
# Flume 源码学习以及阅读
其中大部分源码均添加了注释.

## 三大组件介绍

### source

### channel

### sink


# 附：Flume 官方文档

Apache Flume is a distributed, reliable, and available service for efficiently
collecting, aggregating, and moving large amounts of log data. It has a simple
and flexible architecture based on streaming data flows. It is robust and fault
tolerant with tunable reliability mechanisms and many failover and recovery
mechanisms. The system is centrally managed and allows for intelligent dynamic
management. It uses a simple extensible data model that allows for online
analytic application.

The Apache Flume 1.x (NG) code line is a refactoring of the first generation
Flume to solve certain known issues and limitations of the original design.

Apache Flume is open-sourced under the Apache Software Foundation License v2.0.

## Flume文档

Documentation is included in the binary distribution under the docs directory.
In source form, it can be found in the flume-ng-doc directory.

The Flume 1.x guide and FAQ are available here:

* https://cwiki.apache.org/FLUME
* https://cwiki.apache.org/confluence/display/FLUME/Getting+Started


## 编译 Flume

Compiling Flume requires the following tools:

* Oracle Java JDK 1.8
* Apache Maven 3.x

Note: The Apache Flume build requires more memory than the default configuration.
We recommend you set the following Maven options:

`export MAVEN_OPTS="-Xms512m -Xmx1024m"`

To compile Flume and build a distribution tarball, run `mvn install` from the
top level directory. The artifacts will be placed under `flume-ng-dist/target/`.

