export PYSPARK_PYTHON=python3
export JAVA_HOME=/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
./spark-2.3.0-bin-hadoop2.7/bin/spark-submit --master spark://207.184.161.138:7077 /Users/danielmuraveyko/playground/ir/lab1/main.py 1000