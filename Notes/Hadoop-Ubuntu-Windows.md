# Hadoop 在windows的Ubuntu子系统中的部署

## Steps

1. 在windows中部署好Ubuntu子系统。
2. 在Ubuntu中安装好Java环境。
    1. 先升级apt-get
    ```
        sudo apt-get update
    ```
    2. 查看Java版本
    ```
        java -version
    ```
    3. 安装默认Java版本
    ```
        sudo apt-get install default-gre
    ```
    4. 查看jdk路径：
    ```
        /usr/lib/jvm/java-1.8.0-openjdk-amd64
    ```
    5. 安装ssh.
    ```
        sudo apt-get install ssh
    ```
    6. 安装pdsh.
    ```
        sudo apt-get install pdsh
    ```
3. 下载Hadoop (binary downlaod).

    https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html

    Source download下载完需要自行compile.

4. 解压的时候，需要使用管理员权限，在搜索框中搜索winRAR，右击，管理员身份启动。

5. 关注三个文件夹：
    1. bin下面的hadoop,最好配置成环境变量的路径。
    2. etc文件夹包含配置文件。
    3. share文件夹包含org.apache.hadoop的各种jar包。

6. 给Hadoop配置%JAVA_HOME.

    /hadoop-3.2.1/etc/hadoop/hadoop-env.sh
    找到某行
    ```
        export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64
    ```

7. Ubuntu中测试hadoop命令。
    ```
        $ bin/hadoop
    ```
8. 如果没问题了，就可以开始逐步实现一下几个模式。

    This document describes how to set up and configure **a single-node Hadoop** installation so that you can quickly perform simple operations using Hadoop MapReduce and the Hadoop Distributed File System (HDFS).

    1. **Standalone Operation**: By default, Hadoop is configured to run in a non-distributed mode, as a single Java process. This is useful for debugging.
    2. **Pseudo-Distributed Operation**: Hadoop can also be run on a single-node in a pseudo-distributed mode where each Hadoop daemon runs in a separate Java process.
    3. **Fully-Distributed Operation**: For information on setting up fully-distributed, non-trivial clusters see Cluster Setup.

        https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/ClusterSetup.html

9. 先测试Standalone Operation.
```
  $ mkdir input
  $ cp etc/hadoop/*.xml input
  $ bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-3.2.1.jar grep input output 'dfs[a-z.]+'
  $ cat output/*
```
10. 在Eclipse中建立工程，需要使用Hadoop的jar包，路径是：
```
\share\common和\share\mapreduce
```
11. Hadoop在命令行，只能运行Eclipse的jar包。可以在Eclipse中右击工程文件夹，生成jar包。
12. 如果需要查看Hadoop的jar包源码，需要装一个反编译工具，jd-eclipse，然后Ctrl+左击类名就能看到源码。
