# Apache Hadoop

Setting up a Single Node Cluster.

这个不管用，别看了。

## Purpose
This document describes how to set up and configure **a single-node Hadoop installation** so that you can quickly perform simple operations using Hadoop MapReduce and the Hadoop Distributed File System (HDFS).

## Supported Platforms
- Linux
- WIndows


## Reference
https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html

**To set up Hadoop on Windows, see**
https://cwiki.apache.org/confluence/display/HADOOP2/Hadoop2OnWindows

1. Download stable2:
binary download not source download
http://mirrors.ibiblio.org/apache/hadoop/common/

2. Open winRAR as an administrator.
search -> winRAR -> Right click -> Run as an administrator.

3. Extract the .tar.gz file.

4. Install requirements for windows.
https://svn.apache.org/viewvc/hadoop/common/branches/branch-2/BUILDING.txt?view=markup

- Windows System
- JDK 1.6+
- Maven 3.0 or later
- Findbugs 1.3.9 (if running findbugs)
- ProtocolBuffer 2.5.0 序列化数据结构的协议
- CMake 2.6 or newer
- Windows SDK or Visual Studio 2010 Professional
- Unix command-line tools from GnuWin32 or Cygwin: sh, mkdir, rm, cp, tar, gzip
- zlib headers (if building native code bindings for zlib)
- Internet connection for first build (to fetch all Maven and Hadoop dependencies)

这两个可能都不用装 ---

5. Download window10 SDK.

https://developer.microsoft.com/en-US/windows/downloads/windows-10-sdk/

6. CMake
CMake是个一个开源的跨平台自动化建构系统，用来管理软件建置的程序，并不依赖于某特定编译器，并可支持多层目录、多个应用程序与多个库。 它用配置文件控制建构过程的方式和Unix的make相似，只是CMake的配置文件取名为CMakeLists.txt。

https://cmake.org/download/

安装的时候添加环境变量。

## Starting a Single Node (pseudo-distributed) Cluster
This section describes the absolute minimum configuration required to start a Single Node (pseudo-distributed) cluster and also run an example MapReduce job.

### HDFS Configuration
安装路径/etc

7. hadoop-env.cmd 这里需要配置%USERNAME%吗
8. core-site.xml
9. hdfs-site.xml
10. slaves 这个可能就是workers

### YARN Configuration

11. marred-site.xml

用自己的windows用户名替换%USERNAME%

这里需要小心user的单复数吗？

12. yarn-site.xml

### Initialize Environment Variables
在cmd执行这个文件。
```
c:\deploy\etc\hadoop\hadoop-env.cmd
```
13. 注意一下%JAVA_HOME%环境变量


Make sure that JAVA_HOME is set in your environment and does not contain any spaces. If your default Java installation directory has spaces then you must use the Windows 8.3 Pathname instead e.g. c:\Progra~1\Java\... instead of c:\Program Files\Java\....

这是原来的设置
```
C:\Program Files\Java\jdk1.8.0_211
```
需要改成这样
```
C:\Progra~1\Java\jdk1.8.0_211
```

