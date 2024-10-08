# AutoMQ: A cloud-first alternative of Kafka by decoupling durability to S3 and EBS


<div align="center">
<p align="center">
  🔥&nbsp <a
    href="https://www.automq.com/quick-start#Cloud?utm_source=github_automq_cloud"
    target="_blank"
  ><b>Free trial of AutoMQ Business Edition</b></a>&nbsp&nbsp&nbsp
  📑&nbsp <a
    href="https://docs.automq.com/docs/automq-opensource/HSiEwHVfdiO7rWk34vKcVvcvn2Z?utm_source=github"
    target="_blank"
  ><b>Documentation</b></a>&nbsp&nbsp&nbsp
  📃&nbsp <a
    href="https://www.automq.com/blog/introducing-automq-cloud-native-replacement-of-apache-kafka?utm_source=github"
    target="_blank"
  ><b>AutoMQ Introduction</b></a>
</p>


[![Linkedin Badge](https://img.shields.io/badge/-LinkedIn-blue?style=flat-square&logo=Linkedin&logoColor=white&link=https://www.linkedin.com/company/automq)](https://www.linkedin.com/company/automq)
[![Twitter URL](https://img.shields.io/twitter/follow/AutoMQ)](https://twitter.com/intent/follow?screen_name=AutoMQ_Lab)
[![](https://img.shields.io/badge/-%20Wechat%20-red?style=social&logo=discourse)](docs/images/automq-wechat.png)
[![](https://badgen.net/badge/Slack/Join%20AutoMQ/0abd59?icon=slack)](https://join.slack.com/t/automq/shared_invite/zt-29h17vye9-thf31ebIVL9oXuRdACnOIA)
[![](https://img.shields.io/badge/AutoMQ%20vs.%20Kafka(Cost)-yellow)](https://www.automq.com/blog/automq-vs-apache-kafka-a-real-aws-cloud-bill-comparison)
[![](https://img.shields.io/badge/AutoMQ%20vs.%20Kafka(Performance)-orange)](https://docs.automq.com/docs/automq-opensource/IJLQwnVROiS5cUkXfF0cuHnWnNd)

<a href="https://trendshift.io/repositories/9782" target="_blank"><img src="https://trendshift.io/api/badge/repositories/9782" alt="AutoMQ%2Fautomq | Trendshift" style="width: 250px; height: 55px;" width="250" height="55"/></a>

---

![](https://img.shields.io/badge/AWS-%E2%9C%85-lightgray?logo=amazonaws)
![](https://img.shields.io/badge/Google-%F0%9F%9A%A7-lightyellow?logo=googlecloud)
![](https://img.shields.io/badge/Azure-%F0%9F%9A%A7-lightyellow?logo=microsoftazure)
![](https://img.shields.io/badge/Aliyun-%E2%9C%85-lightgray?logo=alibabacloud)
![](https://img.shields.io/badge/Huawei-%E2%9C%85-lightgray?logo=huawei)
![](https://img.shields.io/badge/Baidu-%E2%9C%85-lightgray?logo=baidu)
![](https://img.shields.io/badge/Tencent-%E2%9C%85-lightgray?logo=tencentqq)
</div>

## 📺 Youtube Video Introduction
<b>Watch this video to learn what is AutoMQ. ⬇️ ⬇️ ⬇️ </b>

[![What is AutoMQ?](https://img.youtube.com/vi/3JQrclZlie4/0.jpg)](https://www.youtube.com/watch?v=3JQrclZlie4)



## 🍵 AutoMQ vs Other Streaming Platforms

<table>
  <tr>
    <th>Feature</th>
    <th>AutoMQ</th>
    <th>Apache Kafka</th>
    <th>Confluent</th>
    <th>Apache Pulsar</th>
    <th>Redpanda</th>
    <th>Warpstream</th>
  </tr>
  <tr>
    <td>Apache Kafka Compatibility[1]</td>
    <td colspan="3">Native Kafka</td>
    <td>Non-Kafka</td>
    <td colspan="2">Kafka Protocol</td>
  </tr>
  <tr>
    <td>Source Code Availability</td>
    <td>Yes</td>
    <td>Yes</td>
    <td>No</td>
    <td>Yes</td>
    <td>Yes</td>
    <td>No</td>
  </tr>
  <tr>
    <td>Stateless Broker</td>
    <td>Yes</td>
    <td>No</td>
    <td>No</td>
    <td>Yes</td>
    <td>No</td>
    <td>Yes</td>
  </tr>
  <tr>
    <td>Publisher Latency(P99)</td>
    <td colspan="5">Single-digit ms latency</td>
    <td><a href="https://www.warpstream.com/blog/warpstream-benchmarks-and-tco">> 620ms</a></td>
  </tr>
  <tr>
    <td>Continuous Self-Balancing</td>
    <td>Yes</td>
    <td>No</td>
    <td>Yes</td>
    <td>Yes</td>
    <td>Yes</td>
    <td>Yes</td>
  </tr>
  <tr>
    <td>Scale in/out</td>
    <td>In seconds</td>
    <td>In hours/days</td>
    <td>In hours</td>
    <td>In hours<br>(scale-in);<br> In seconds<br>(scale-out)</td>
    <td>In hours<br>In seconds (Enterprise Only)</td>
    <td>In seconds</td>
  </tr>
  <tr>
    <td>Spot Instance Support</td>
    <td>Yes</td>
    <td>No</td>
    <td>No</td>
    <td>No</td>
    <td>No</td>
    <td>Yes</td>
  </tr>
  <tr>
    <td>Partition Reassignment</td>
    <td>In seconds</td>
    <td>In hours/days</td>
    <td>In hours</td>
    <td>In seconds</td>
    <td>In hours<br>In seconds (Enterprise Only)</td>
    <td>In seconds</td>
  </tr>
  <tr>
    <td>Component</td>
    <td>Broker</td>
    <td colspan="2">Broker<br>Zookeeper<br>(Non-KRaft)</td>
    <td>Broker<br>Zookeeper<br>Bookkeeper<br>Proxy(Optional)</td>
    <td>Broker</td>
    <td>Agent<br>MetadataServer</td>
  </tr>
  <tr>
    <td>Durability</td>
    <td>Guaranteed by S3/EBS[2]</td>
    <td colspan="2">Guaranteed by ISR </td>
    <td>Guaranteed by Bookkeeper</td>
    <td>Guaranteed by Raft</td>
    <td>Guaranteed by S3</td>
  </tr>
  <tr>
    <td>Inter-AZ Networking Fees</td>
    <td>No</td>
    <td colspan="4">Yes</td>
    <td>No</td>
  </tr>
</table>


> [1] Apache Kafka Compatibility's definition is coming from this [blog](https://www.kai-waehner.de/blog/2021/05/09/kafka-api-de-facto-standard-event-streaming-like-amazon-s3-object-storage/).

> [2] EBS Durability: On Azure, GCP, and Alibaba Cloud, Regional EBS replicas span multiple AZs. On AWS, ensure durability by double writing to EBS and S3 Express One Zone in different AZs.

## 🔶 Why AutoMQ

- **Cost effective**: The first true cloud-native streaming storage system, designed for optimal cost and efficiency on the cloud. Refer to [this report](https://docs.automq.com/docs/automq-opensource/EV6mwoC95ihwRckMsUKcppnqnJb) to see how we cut Apache Kafka billing by 90% on the cloud.
- **High Reliability**: Leverage cloud-shared storage services(EBS and S3) to achieve zero RPO, RTO in seconds and 99.999999999% durability.
- **Serverless**:
  - Auto Scaling: Monitor cluster metrics and automatically scale in/out to align with your workload, enabling a pay-as-you-go model.
  - Scaling in seconds: The computing layer (broker) is stateless and can scale in/out within seconds, making AutoMQ a truly serverless solution.
  - Infinite scalable: Utilize cloud object storage as the primary storage solution, eliminating concerns about storage capacity.
- **Manage-less**: The built-in auto-balancer component automatically schedules partitions and network traffic between brokers, eliminating manual partition reassignment.
- **High performance**:
  - Low latency: Accelerate writing with high-performance EBS as WAL, achieving single-digit millisecond latency.
  - High throughput: Leverage pre-fetching, batch processing, and parallel technologies to maximize the capabilities of cloud object storage.
  > Refer to the [AutoMQ Performance White Paper](https://docs.automq.com/docs/automq-opensource/IJLQwnVROiS5cUkXfF0cuHnWnNd) to see how we achieve this.
- **A superior alternative to Apache Kafka**: 100% compatible with Apache Kafka and does not lose any key features, but cheaper and better.

## ✨Architecture

![image](./docs/images/automq_vs_kafka.gif)

AutoMQ adopts a Shared-Storage architecture, replacing the storage layer of Apache Kafka with a shared streaming storage library called [S3Stream](https://github.com/AutoMQ/automq/tree/main/s3stream) in a storage-compute separation manner, making the Broker completely stateless.

Compared to the classic Kafka Shared-Nothing or Tiered-Storage architectures, AutoMQ's computing layer (Broker) is truly stateless, enabling features such as Auto-Scaling, Self-Balancing, and Partition Reassignment in Seconds that significantly reduce costs and improve efficiency.

## ⛄ Get started with AutoMQ

### Deploy Locally on a Single Host
```
curl https://download.automq.com/community_edition/standalone_deployment/install_run.sh | bash
```

The easiest way to run AutoMQ. You can experience features like **Partition Reassignment in Seconds** and **Continuous Self-Balancing** in your local machine. [Learn more](https://docs.automq.com/docs/automq-opensource/EsUBwQei4ilCDjkWb8WcbOZInwc)

There are more deployment options available:
- [Deploy on Linux with 5 Nodes](https://docs.automq.com/docs/automq-opensource/IyXrw3lHriVPdQkQLDvcPGQdnNh)
- [Deploy on Kubernetes(Enterprise Edition Only)](https://docs.automq.com/docs/automq-opensource/KJtLwvdaPi7oznkX3lkcCR7fnte)
- [Runs on Ceph / MinIO / CubeFS / HDFS](https://docs.automq.com/docs/automq-opensource/RexrwfhKuiGChfk237QcEBIwnND)
- [Try AutoMQ on Alibaba Cloud Marketplace](https://market.aliyun.com/products/55530001/cmgj00065841.html)
- [Try AutoMQ on AWS Marketplace](https://docs.automq.com/automq-cloud/getting-started/install-byoc-environment/aws/install-env-from-marketplace)

## 💬 Community
You can join the following groups or channels to discuss or ask questions about AutoMQ:
- Ask questions or report a bug by [GitHub Issues](https://github.com/AutoMQ/automq/issues)
- Discuss about AutoMQ or Kafka by [Slack](https://join.slack.com/t/automq/shared_invite/zt-29h17vye9-thf31ebIVL9oXuRdACnOIA) or [Wechat Group](docs/images/automq-wechat.png)


## 👥 How to contribute
If you've found a problem with AutoMQ, please open a [GitHub Issues](https://github.com/AutoMQ/automq/issues).
To contribute to AutoMQ please see [Code of Conduct](CODE_OF_CONDUCT.md) and [Contributing Guide](CONTRIBUTING_GUIDE.md).
We have a list of [good first issues](https://github.com/AutoMQ/automq/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) that help you to get started, gain experience, and get familiar with our contribution process. To claim one, simply reply with 'pick up' in the issue and the AutoMQ maintainers will assign the issue to you. If you have any questions about the 'good first issue' please feel free to ask. We will do our best to clarify any doubts you may have.

## AutoMQ Business Edition
The business edition of AutoMQ provides a powerful and easy-to-use control plane to help you manage clusters effortlessly. Meanwhile, the control plane is more powerful in terms of availability and observability compared to the community edition.

> You can check the difference between the community and business editions [here](https://www.automq.com/product).


<b>Watch the following video and refer to our [docs](https://docs.automq.com/automq-cloud/getting-started/install-byoc-environment/aws/install-env-via-terraform-module) to see how to deploy AutoMQ Business Edition with 2 weeks free license for PoC.</b>

<b> ⬇️ ⬇️ ⬇️ </b>

[![Deploy AutoMQ Business Edition with Terraform](https://img.youtube.com/vi/O40zp81x97w/0.jpg)](https://www.youtube.com/watch?v=O40zp81x97w)



### Free trial of AutoMQ Business Edition
To allow users to experience the capabilities of the AutoMQ business edition without any barriers, click [here](https://www.automq.com/quick-start#Cloud?utm_source=github_automq_cloud) to apply for a no-obligation cluster trial, and note `AutoMQ Cloud Free Trial` in the message input box. We will immediately initialize an AutoMQ Cloud control panel for you soon in the cloud and give you the address of the control panel. Then, you can use the control panel to create a AutoMQ cluster or perform operations like scale in/out. 

No need to bind a credit card, no cost at all. We look forward to receiving valuable feedback from you to make our product better. If you want to proceed with a formal POC, you can also contact us through [Contact Us](https://automq66.feishu.cn/share/base/form/shrcnoqxslhYkujx6ULiMxOqkGh?utm_source=github_poc). We will further support your official POC.

## 👥 AutoMQ Use Cases
> Here are some of the users who have deployed AutoMQ in their production environments.

<<<<<<< HEAD
[![Click on the link to learn more about AutoMQ user cases.](./docs/images/customer.jpeg "AutoMQ Uses Case")](https://www.automq.com/customer)
## 🙋 Contact Us
Want to learn more, [Talk with our product experts](https://automq66.feishu.cn/share/base/form/shrcnoqxslhYkujx6ULiMxOqkGh).
=======
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

### Running a particular unit/integration test ###
    ./gradlew clients:test --tests RequestResponseTest

### Repeatedly running a particular unit/integration test ###
    I=0; while ./gradlew clients:test --tests RequestResponseTest --rerun --fail-fast; do (( I=$I+1 )); echo "Completed run: $I"; sleep 1; done

### Running a particular test method within a unit/integration test ###
    ./gradlew core:test --tests kafka.api.ProducerFailureHandlingTest.testCannotSendToInternalTopic
    ./gradlew clients:test --tests org.apache.kafka.clients.MetadataTest.testTimeToNextUpdate

### Running a particular unit/integration test with log4j output ###
By default, there will be only small number of logs output while testing. You can adjust it by changing the `log4j.properties` file in the module's `src/test/resources` directory.

For example, if you want to see more logs for clients project tests, you can modify [the line](https://github.com/apache/kafka/blob/trunk/clients/src/test/resources/log4j.properties#L21) in `clients/src/test/resources/log4j.properties` 
to `log4j.logger.org.apache.kafka=INFO` and then run:
    
    ./gradlew cleanTest clients:test --tests NetworkClientTest   

And you should see `INFO` level logs in the file under the `clients/build/test-results/test` directory.

### Specifying test retries ###
By default, each failed test is retried once up to a maximum of five retries per test run. Tests are retried at the end of the test task. Adjust these parameters in the following way:

    ./gradlew test -PmaxTestRetries=1 -PmaxTestRetryFailures=5
    
See [Test Retry Gradle Plugin](https://github.com/gradle/test-retry-gradle-plugin) for more details.

### Generating test coverage reports ###
Generate coverage reports for the whole project:

    ./gradlew reportCoverage -PenableTestCoverage=true -Dorg.gradle.parallel=false

Generate coverage for a single module, i.e.: 

    ./gradlew clients:reportCoverage -PenableTestCoverage=true -Dorg.gradle.parallel=false
    
### Building a binary release gzipped tar ball ###
    ./gradlew clean releaseTarGz

The release file can be found inside `./core/build/distributions/`.

### Building auto generated messages ###
Sometimes it is only necessary to rebuild the RPC auto-generated message data when switching between branches, as they could
fail due to code changes. You can just run:
 
    ./gradlew processMessages processTestMessages

### Running a Kafka broker in KRaft mode

Using compiled files:

    KAFKA_CLUSTER_ID="$(./bin/kafka-storage.sh random-uuid)"
    ./bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
    ./bin/kafka-server-start.sh config/kraft/server.properties

Using docker image:

    docker run -p 9092:9092 apache/kafka:3.7.0

### Running a Kafka broker in ZooKeeper mode

Using compiled files:

    ./bin/zookeeper-server-start.sh config/zookeeper.properties
    ./bin/kafka-server-start.sh config/server.properties

>Since ZooKeeper mode is already deprecated and planned to be removed in Apache Kafka 4.0, the docker image only supports running in KRaft mode

### Cleaning the build ###
    ./gradlew clean

### Running a task with one of the Scala versions available (2.12.x or 2.13.x) ###
*Note that if building the jars with a version other than 2.13.x, you need to set the `SCALA_VERSION` variable or change it in `bin/kafka-run-class.sh` to run the quick start.*

You can pass either the major version (eg 2.12) or the full version (eg 2.12.7):

    ./gradlew -PscalaVersion=2.12 jar
    ./gradlew -PscalaVersion=2.12 test
    ./gradlew -PscalaVersion=2.12 releaseTarGz

### Running a task with all the scala versions enabled by default ###

Invoke the `gradlewAll` script followed by the task(s):

    ./gradlewAll test
    ./gradlewAll jar
    ./gradlewAll releaseTarGz

### Running a task for a specific project ###
This is for `core`, `examples` and `clients`

    ./gradlew core:jar
    ./gradlew core:test

Streams has multiple sub-projects, but you can run all the tests:

    ./gradlew :streams:testAll

### Listing all gradle tasks ###
    ./gradlew tasks

### Building IDE project ####
*Note that this is not strictly necessary (IntelliJ IDEA has good built-in support for Gradle projects, for example).*

    ./gradlew eclipse
    ./gradlew idea

The `eclipse` task has been configured to use `${project_dir}/build_eclipse` as Eclipse's build directory. Eclipse's default
build directory (`${project_dir}/bin`) clashes with Kafka's scripts directory and we don't use Gradle's build directory
to avoid known issues with this configuration.

### Publishing the jar for all versions of Scala and for all projects to maven ###
The recommended command is:

    ./gradlewAll publish

For backwards compatibility, the following also works:

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

    ./gradlew checkstyleMain checkstyleTest spotlessCheck

The checkstyle warnings will be found in `reports/checkstyle/reports/main.html` and `reports/checkstyle/reports/test.html` files in the
subproject build directories. They are also printed to the console. The build will fail if Checkstyle fails.

**Please note that `./gradlew spotlessCheck` currently has an issue with Java 21 (see https://github.com/diffplug/spotless/pull/1920), so make sure to run this with JDK 11 or 17**

#### Spotless ####
The import order is a part of static check. please call `spotlessApply` (require JDK 11+) to optimize the imports of Java codes before filing pull request.

    ./gradlew spotlessApply

**Please note that `./gradlew spotlessApply` currently has an issue with Java 21 (see https://github.com/diffplug/spotless/pull/1920), so make sure to run this with JDK 11 or 17**

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
>>>>>>> 3.9.0
