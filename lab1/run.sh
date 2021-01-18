export PYSPARK_PYTHON=python3
export SPARK_HOME=/Users/danielmuraveyko/playground/ir/lab1/spark-2.3.0-bin-hadoop2.7
export JAVA_HOME=/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

$SPARK_HOME/sbin/start-master.sh && $SPARK_HOME/sbin/start-slave.sh spark://MBP-Daniel:7077

$SPARK_HOME/bin/spark-submit  /Users/danielmuraveyko/playground/ir/lab1/main.py

 $SPARK_HOME/sbin/stop-slave.sh  && $SPARK_HOME/sbin/stop-master.sh  