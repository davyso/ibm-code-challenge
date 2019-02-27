# ibm-code-challenge (Spark)

## Setup
For Windows machines, the global Hadoop library is needed, which it appears to already exist in MacOS. Fortunately, that 
Hadoop library is bootstrapped within this project. You will just need to set the following environment variable in 
IntelliJ (`Run` -> `Edit Configurations...`):
```
HADOOP_HOME=$PROJECT_DIR$\hadoop-2.7.1\
```
The other dependencies of the tech stack (Scala, Spark, etc.) should be setup using IntelliJ's provided Maven.

## Running Jumbles
The class `DeJumblerTest` contains all 5 Jumble puzzle examples. You can run all of them at once or one at a time. The 
test suite also contains unit tests that you may choose to run.