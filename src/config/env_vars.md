If you want to reduce the findspark dependency, you can just make sure you have these variables in your .bashrc
```sh
export SPARK_HOME='/opt/spark2.4'
export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
export PYSPARK_DRIVER_PYTHON="jupyter"
export PYSPARK_DRIVER_PYTHON_OPTS="notebook"
export PYSPARK_PYTHON=python3
export PATH=$SPARK_HOME:$PATH:~/.local/bin:$JAVA_HOME/bin:$JAVA_HOME/jre/bin
```


Pure python solution, add this code on top of your jupyter notebook (maybe in the first cell):

```python
import os
import sys
os.environ["PYSPARK_PYTHON"] = "/opt/continuum/anaconda/bin/python"
os.environ["SPARK_HOME"] = "/opt/spark2.4"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.9-src.zip")
sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")
```