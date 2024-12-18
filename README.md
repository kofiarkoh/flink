## About This Project
This repo is a fork of [Apache Flink](https://github.com/apache/flink) in which  [openctest](https://www.usenix.org/conference/osdi20/presentation/sun) configuration testing is applied using [cTest4j Tool](https://dl.acm.org/doi/10.1145/3663529.3663799)


## System Requirement
- Java installation. (This project has been test on Java 17 but can it may also be run on Java 11)
- Maven must be installed. You may check by running `mvn -v` in your terminal. We have ran this against Maven 3.9

## Runninng  Configuration Tests

> **NOTE:** The steps below have been run using **openjdk version 17.0.10** on **macOS Sequoia**.


For demonstration purposes, the steps below simulates testing configuration tests induced failures 
using `TimeWindowTranslationTest` test class inside the `flink-runtime` submodule _only_.
- Run `wget -O archive.zip https://zenodo.org/records/14347656/files/Archive.zip?download=1` . This is a huge file so expect some delay (about 7 mins)
- Run `unzip archive.zip -d .m2`
- Clone the project by running the command `git clone https://github.com/kofiarkoh/flink.git`
- Switch to the project directory by running `cd flink/flink-runtime`
- Run `mvn clean install -DskipTests  -Denforcer.skip=true -Drat.skip=true` to build the project
- Run the test below using default configuration values as specified in the test file. This test will PASS
```
 mvn surefire:test -Denforcer.skip=true -Dctest.config.save -Dmaven.test.failure.ignore=true -Dtest="org.apache.flink.streaming.runtime.operators.windowing.TimeWindowTranslationTest" -Drat.skip=true
```
![Failing Tests](./img/pass.png)
- Now lets change one conguration value used in the tests (this value is a valid configuration value). This test will PASS
```
mvn surefire:test -Denforcer.skip=true -Dctest.config.save -Dmaven.test.failure.ignore=true -Dtest="org.apache.flink.streaming.runtime.operators.windowing.TimeWindowTranslationTest" -Dconfig.inject.cli="parallelism.default=1" -Drat.skip=true
```
- To simulate a configuration induced test failure. Run the command below. _For this step, we set `parallelism.default` to an invalid value `1s` because valid value must be an integer. This test will FAIL because an invalid configuration value is injected into the test case
```
 mvn surefire:test -Denforcer.skip=true -Dctest.config.save -Dmaven.test.failure.ignore=true -Dtest="org.apache.flink.streaming.runtime.operators.windowing.TimeWindowTranslationTest" -Dconfig.inject.cli="parallelism.default=1s" -Drat.skip=true

```
![Failing Tests](./img/fail.png)
