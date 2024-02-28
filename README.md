<<<<<<< HEAD
<h1 align="center">
AutoMQ: Truly serverless Kafka solution that maximizes the benefits of cloud
</h1>
<h3 align="center">
    
</h3>
=======
Apache Kafka
=================
See our [web site](https://kafka.apache.org) for details on the project.

You need to have [Java](http://www.oracle.com/technetwork/java/javase/downloads/index.html) installed.

We build and test Apache Kafka with Java 8, 11, 17 and 21. We set the `release` parameter in javac and scalac
to `8` to ensure the generated binaries are compatible with Java 8 or higher (independently of the Java version
used for compilation). Java 8 support project-wide has been deprecated since Apache Kafka 3.0, Java 11 support for
the broker and tools has been deprecated since Apache Kafka 3.7 and removal of both is planned for Apache Kafka 4.0 (
see [KIP-750](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=181308223) and
[KIP-1013](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=284789510) for more details).

Scala 2.12 and 2.13 are supported and 2.13 is used by default. Scala 2.12 support has been deprecated since
Apache Kafka 3.0 and will be removed in Apache Kafka 4.0 (see [KIP-751](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=181308218)
for more details). See below for how to use a specific Scala version or all of the supported Scala versions.

### Build a jar and run it ###
    ./gradlew jar

Follow instructions in https://kafka.apache.org/quickstart

### Build source jar ###
    ./gradlew srcJar

### Build aggregated javadoc ###
    ./gradlew aggregatedJavadoc

### Build javadoc and scaladoc ###
    ./gradlew javadoc
    ./gradlew javadocJar # builds a javadoc jar for each module
    ./gradlew scaladoc
    ./gradlew scaladocJar # builds a scaladoc jar for each module
    ./gradlew docsJar # builds both (if applicable) javadoc and scaladoc jars for each module

### Run unit/integration tests ###
    ./gradlew test # runs both unit and integration tests
    ./gradlew unitTest
    ./gradlew integrationTest
    
### Force re-running tests without code change ###
    ./gradlew test --rerun
    ./gradlew unitTest --rerun
    ./gradlew integrationTest --rerun
>>>>>>> trunk

![GitHub License](https://img.shields.io/github/license/AutoMQ/automq-for-kafka)
![GitHub release (with filter)](https://img.shields.io/github/v/release/AutoMQ/automq-for-kafka)
![GitHub pull requests](https://img.shields.io/github/issues-pr/AutoMQ/automq-for-kafka)
![GitHub closed pull requests](https://img.shields.io/github/issues-pr-closed/AutoMQ/automq-for-kafka)
![GitHub all releases](https://img.shields.io/github/downloads/AutoMQ/automq-for-kafka/total)
![](https://img.shields.io/badge/Java%20-%20JDK17-green)

<<<<<<< HEAD
---
[![](https://img.shields.io/badge/official%20website-20B2AA?style=for-the-badge)](https://www.automq.com)
&nbsp;
[![](https://img.shields.io/badge/official%20document-blue?style=for-the-badge)](https://docs.automq.com/docs/automq-s3kafka/YUzOwI7AgiNIgDk1GJAcu6Uanog)
&nbsp;
<a href="https://twitter.com/AutoMQ_Lab" target="_blank"><img src="https://img.shields.io/badge/- @AutoMQ_Lab -424549?style=social&logo=twitter" height=25></a>
&nbsp;
<a href="docs/images/automq-wechat.png" target="_blank"><img src="https://img.shields.io/badge/- Wechat -red?style=social&logo=discourse" height=25></a>
&nbsp;
=======
### Repeatedly running a particular unit/integration test ###
    I=0; while ./gradlew clients:test --tests RequestResponseTest --rerun --fail-fast; do (( I=$I+1 )); echo "Completed run: $I"; sleep 1; done
>>>>>>> trunk

---

<img src="https://img.shields.io/badge/aws%20cloud-supported-lightgreen?style=for-the-badge&logo=amazonaws" height="18"><img src="https://img.shields.io/badge/google%20cloud-todo-lightyellow?style=for-the-badge&logo=googlecloud" height="18"><img src="https://img.shields.io/badge/Azure%20cloud-todo-lightyellow?style=for-the-badge&logo=microsoftazure" height="18"><img src="https://img.shields.io/badge/aliyun%20cloud-supported-lightgreen?style=for-the-badge&logo=alibabacloud" height="18"><img src="https://img.shields.io/badge/huawei%20cloud-supported-lightgreen?style=for-the-badge&logo=huawei" height="18"><img src="https://img.shields.io/badge/baidu%20cloud-supported-lightgreen?style=for-the-badge&logo=baidu" height="18"><img src="https://img.shields.io/badge/tencent%20cloud-supported-lightgreen?style=for-the-badge&logo=tencentqq" height="18">



![image](./docs/images/banner-readme.jpeg)



[//]: # ([![E2E_TEST]&#40;https://github.com/AutoMQ/automq-for-kafka/actions/workflows/nightly-e2e.yml/badge.svg&#41;]&#40;https://github.com/AutoMQ/automq-for-kafka/actions/workflows/nightly-e2e.yml&#41;)

## 🍵What is AutoMQ

AutoMQ is a cloud-native, serverless reinvented Kafka that is easily scalable, manage-less and cost-effective.


## 🔶Why AutoMQ

- **Cloud Native**: Built on cloud service. Every system design decision take cloud service's feature and billing items into consideration to offer best low-latency, scalable, reliable and cost-effective Kafka service on cloud.
- **Serverless**:
  - Auto Scaling: Watch key metrics of cluster and scale in/out automatically to match you workload and achieve pay-as-you-go.
  - Scaling in seconds: Computing layer (broker) is stateless and could scale in/out in seconds, which make AutoMQ true serverless. [Learn more](https://docs.automq.com/docs/automq-s3kafka/Eo4Bweg4eiPegykLpAycED1yn7g)
  - Infinite scalable: Use cloud's object storage as the main storage, never worry about storage capacity.
- **Manage-less**: Built-in auto-balancer component balance partition and network traffic across brokers automatically. Never worry about partition re-balance. [Learn more](https://docs.automq.com/docs/automq-s3kafka/GSN2wZjeWiR70YkZiRsc6Hqsneh)
- **Cost effective**: Use object storage as the main storage, take billing items into consideration when design system, fully utilize the cloud service, all of them contribute to AutoMQ and make it 10x cheaper than Apache Kafka. Refer to [this report](https://docs.automq.com/docs/automq-s3kafka/EJBvwM3dNic6uYkZAWwc7nmrnae) to see how we cut Apache Kafka billing by 90% on the cloud.
- **High performance**: 
  - Low latency: Use cloud block storage like AWS EBS as the durable cache layer to accelerate write.
  - High throughput: Use pre-fetching, batch processing and parallel to achieve high throughput.
  > Refer to the [AutoMQ Performance White Paper](https://docs.automq.com/docs/automq-s3kafka/CYxlwqDBHitThCkxSl2cePxrnBc) to see how we achieve this.
- **A superior alternative to Apache Kafka**: 100% compatible with Apache Kafka greater than 0.9.x and not lose any good features of it, but cheaper and better.



## ✨Architecture

![image](./docs/images/automq-architecture.png)

AutoMQ use logSegment as a code aspect of Apache Kafka to weave into our features. The architecture including the following main components:
- **S3Stream**: A streaming library based on object storage offered by AutoMQ. It is the core component of AutoMQ and is responsible for reading and writing data to object storage. [Learn more](https://docs.automq.com/docs/automq-s3kafka/Q8fNwoCDGiBOV6k8CDSccKKRn9d).
- **Stream**: Stream is an abstraction to mapping the logSegment of Apache Kafka. LogSegment's data, index and other meta will mapping to different type of stream. [Learn more](https://docs.automq.com/docs/automq-s3kafka/GUk7w0ZxniPwN7kUgiicIlHkn9d)
- **Durable Cache Layer**: AutoMQ use a small size cloud block storage like AWS EBS as the durable cache layer to accelerate write. Pay attention that this is not tiered storage and AutoMQ broker can decoupled from the durable cache layer completely. [Learn more](https://docs.automq.com/docs/automq-s3kafka/X1DBwDdzWiCMmYkglGHcKdjqn9f)
- **Stream Object**: AutoMQ's data is organized by stream object. Data is read by stream object id through index. One stream have one stream object. [Learn more](https://docs.automq.com/docs/automq-s3kafka/Q8fNwoCDGiBOV6k8CDSccKKRn9d)
- **Stream set object**: Stream set object is a collection of small stream object that aimed to decrease API invoke times and metadata size. [Learn more](https://docs.automq.com/docs/automq-s3kafka/Q8fNwoCDGiBOV6k8CDSccKKRn9d)


## ⛄Get started with AutoMQ

### Run AutoMQ local without cloud
The easiest way to run AutoMQ. You can experience the feature like fast partition move and network traffic auto-balance. [Learn more](https://docs.automq.com/docs/automq-s3kafka/SMKbwchB3i0Y0ykFm75c0ftAnCc)

> Attention: Local mode mock object storage locally and is not a production ready deployment. It is only for demo and test purpose.


### Run AutoMQ on cloud manually
Deploy AutoMQ manually with released tgz files on cloud, currently compatible with AWS, Aliyun Cloud, Tencent Cloud, Huawei Cloud and Baidu Cloud. [Learn more]( https://docs.automq.com/docs/automq-s3kafka/NBo6wwth3iWUIkkNAbYcPg0mnae)

## 💬Community
You can join the following groups or channels to discuss or ask questions about AutoMQ:
- Ask questions or report bug by [GitHub Issues](https://github.com/AutoMQ/automq-for-kafka)
- Discuss about AutoMQ or Kafka by [Slack](https://join.slack.com/t/automq/shared_invite/zt-29h17vye9-thf31ebIVL9oXuRdACnOIA) or [Wechat Group](https://www.automq.com/img/----------------------------1.png)


## 👥How to contribute
If you've found a problem with AutoMQ, please open a [GitHub Issues](https://github.com/AutoMQ/automq-for-kafka). 
To contribute to AutoMQ please see [Code of Conduct](CODE_OF_CONDUCT.md) and [Contributing Guide](CONTRIBUTING_GUIDE.md).
We have a list of [good first issues](https://github.com/AutoMQ/automq-for-kafka/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) that help you to get started, gain experience, and get familiar with our contribution process.

## 🌈Roadmap

Coming soon...

## ⭐License
AutoMQ is released under [Business Source License 1.1](BSL.md). When contributing to AutoMQ, you can find the relevant license header in each file.




<<<<<<< HEAD
=======
    ./gradlewAll uploadArchives

Please note for this to work you should create/update `${GRADLE_USER_HOME}/gradle.properties` (typically, `~/.gradle/gradle.properties`) and assign the following variables

    mavenUrl=
    mavenUsername=
    mavenPassword=
    signing.keyId=
    signing.password=
    signing.secretKeyRingFile=

### Publishing the streams quickstart archetype artifact to maven ###
For the Streams archetype project, one cannot use gradle to upload to maven; instead the `mvn deploy` command needs to be called at the quickstart folder:

    cd streams/quickstart
    mvn deploy

Please note for this to work you should create/update user maven settings (typically, `${USER_HOME}/.m2/settings.xml`) to assign the following variables

    <settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
                           https://maven.apache.org/xsd/settings-1.0.0.xsd">
    ...                           
    <servers>
       ...
       <server>
          <id>apache.snapshots.https</id>
          <username>${maven_username}</username>
          <password>${maven_password}</password>
       </server>
       <server>
          <id>apache.releases.https</id>
          <username>${maven_username}</username>
          <password>${maven_password}</password>
        </server>
        ...
     </servers>
     ...


### Installing ALL the jars to the local Maven repository ###
The recommended command to build for both Scala 2.12 and 2.13 is:

    ./gradlewAll publishToMavenLocal

For backwards compatibility, the following also works:

    ./gradlewAll install

### Installing specific projects to the local Maven repository ###

    ./gradlew -PskipSigning=true :streams:publishToMavenLocal
    
If needed, you can specify the Scala version with `-PscalaVersion=2.13`.

### Building the test jar ###
    ./gradlew testJar

### Running code quality checks ###
There are two code quality analysis tools that we regularly run, spotbugs and checkstyle.

#### Checkstyle ####
Checkstyle enforces a consistent coding style in Kafka.
You can run checkstyle using:

    ./gradlew checkstyleMain checkstyleTest

The checkstyle warnings will be found in `reports/checkstyle/reports/main.html` and `reports/checkstyle/reports/test.html` files in the
subproject build directories. They are also printed to the console. The build will fail if Checkstyle fails.

#### Spotbugs ####
Spotbugs uses static analysis to look for bugs in the code.
You can run spotbugs using:

    ./gradlew spotbugsMain spotbugsTest -x test

The spotbugs warnings will be found in `reports/spotbugs/main.html` and `reports/spotbugs/test.html` files in the subproject build
directories.  Use -PxmlSpotBugsReport=true to generate an XML report instead of an HTML one.

### JMH microbenchmarks ###
We use [JMH](https://openjdk.java.net/projects/code-tools/jmh/) to write microbenchmarks that produce reliable results in the JVM.
    
See [jmh-benchmarks/README.md](https://github.com/apache/kafka/blob/trunk/jmh-benchmarks/README.md) for details on how to run the microbenchmarks.

### Dependency Analysis ###

The gradle [dependency debugging documentation](https://docs.gradle.org/current/userguide/viewing_debugging_dependencies.html) mentions using the `dependencies` or `dependencyInsight` tasks to debug dependencies for the root project or individual subprojects.

Alternatively, use the `allDeps` or `allDepInsight` tasks for recursively iterating through all subprojects:

    ./gradlew allDeps

    ./gradlew allDepInsight --configuration runtimeClasspath --dependency com.fasterxml.jackson.core:jackson-databind

These take the same arguments as the builtin variants.

### Determining if any dependencies could be updated ###
    ./gradlew dependencyUpdates

### Common build options ###

The following options should be set with a `-P` switch, for example `./gradlew -PmaxParallelForks=1 test`.

* `commitId`: sets the build commit ID as .git/HEAD might not be correct if there are local commits added for build purposes.
* `mavenUrl`: sets the URL of the maven deployment repository (`file://path/to/repo` can be used to point to a local repository).
* `maxParallelForks`: maximum number of test processes to start in parallel. Defaults to the number of processors available to the JVM.
* `maxScalacThreads`: maximum number of worker threads for the scalac backend. Defaults to the lowest of `8` and the number of processors
available to the JVM. The value must be between 1 and 16 (inclusive). 
* `ignoreFailures`: ignore test failures from junit
* `showStandardStreams`: shows standard out and standard error of the test JVM(s) on the console.
* `skipSigning`: skips signing of artifacts.
* `testLoggingEvents`: unit test events to be logged, separated by comma. For example `./gradlew -PtestLoggingEvents=started,passed,skipped,failed test`.
* `xmlSpotBugsReport`: enable XML reports for spotBugs. This also disables HTML reports as only one can be enabled at a time.
* `maxTestRetries`: maximum number of retries for a failing test case.
* `maxTestRetryFailures`: maximum number of test failures before retrying is disabled for subsequent tests.
* `enableTestCoverage`: enables test coverage plugins and tasks, including bytecode enhancement of classes required to track said
coverage. Note that this introduces some overhead when running tests and hence why it's disabled by default (the overhead
varies, but 15-20% is a reasonable estimate).
* `keepAliveMode`: configures the keep alive mode for the Gradle compilation daemon - reuse improves start-up time. The values should 
be one of `daemon` or `session` (the default is `daemon`). `daemon` keeps the daemon alive until it's explicitly stopped while
`session` keeps it alive until the end of the build session. This currently only affects the Scala compiler, see
https://github.com/gradle/gradle/pull/21034 for a PR that attempts to do the same for the Java compiler.
* `scalaOptimizerMode`: configures the optimizing behavior of the scala compiler, the value should be one of `none`, `method`, `inline-kafka` or
`inline-scala` (the default is `inline-kafka`). `none` is the scala compiler default, which only eliminates unreachable code. `method` also
includes method-local optimizations. `inline-kafka` adds inlining of methods within the kafka packages. Finally, `inline-scala` also
includes inlining of methods within the scala library (which avoids lambda allocations for methods like `Option.exists`). `inline-scala` is
only safe if the Scala library version is the same at compile time and runtime. Since we cannot guarantee this for all cases (for example, users
may depend on the kafka jar for integration tests where they may include a scala library with a different version), we don't enable it by
default. See https://www.lightbend.com/blog/scala-inliner-optimizer for more details.

### Running system tests ###

See [tests/README.md](tests/README.md).

### Running in Vagrant ###

See [vagrant/README.md](vagrant/README.md).

### Contribution ###

Apache Kafka is interested in building the community; we would welcome any thoughts or [patches](https://issues.apache.org/jira/browse/KAFKA). You can reach us [on the Apache mailing lists](http://kafka.apache.org/contact.html).

To contribute follow the instructions here:
 * https://kafka.apache.org/contributing.html 
>>>>>>> trunk
