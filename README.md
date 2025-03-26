# AutoMQ: A cloud-native alternative of Kafka by decoupling durability to cloud storage services like S3


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
[![](https://badgen.net/badge/Slack/Join%20AutoMQ/0abd59?icon=slack)](https://join.slack.com/t/automq/shared_invite/zt-29h17vye9-thf31ebIVL9oXuRdACnOIA)
[![](https://img.shields.io/badge/AutoMQ%20vs.%20Kafka(Cost)-yellow)](https://www.automq.com/blog/automq-vs-apache-kafka-a-real-aws-cloud-bill-comparison)
[![](https://img.shields.io/badge/AutoMQ%20vs.%20Kafka(Performance)-orange)](https://docs.automq.com/docs/automq-opensource/IJLQwnVROiS5cUkXfF0cuHnWnNd)
[![Gurubase](https://img.shields.io/badge/Gurubase-Ask%20AutoMQ%20Guru-006BFF)](https://gurubase.io/g/automq)

<a href="https://trendshift.io/repositories/9782" target="_blank"><img src="https://trendshift.io/api/badge/repositories/9782" alt="AutoMQ%2Fautomq | Trendshift" style="width: 250px; height: 55px;" width="250" height="55"/></a>

---

![](https://img.shields.io/badge/AWS-%E2%9C%85-lightgray?logo=amazonaws)
![](https://img.shields.io/badge/Google-%E2%9C%85-lightgray?logo=googlecloud)
![](https://img.shields.io/badge/Azure-%E2%9C%85-lightgray?logo=microsoftazure)
![](https://img.shields.io/badge/Aliyun-%E2%9C%85-lightgray?logo=alibabacloud)
</div>

## 👥 Big Companies Worldwide are Using AutoMQ
> Here are some of our customers who are using AutoMQ from all over the world.
<img width="1151" alt="image" src="https://github.com/user-attachments/assets/a2668e5e-eebf-479a-b85a-9611de1b60c8" />

- [Grab: Driving Efficiency with AutoMQ in DataStreaming Platform](https://www.youtube.com/watch?v=IB8sh639Rsg)
- [Palmpay Uses AutoMQ to Replace Kafka, Optimizing Costs by 50%+](https://www.automq.com/blog/palmpay-uses-automq-to-replace-kafka)
- [AutoMQ help Geely Auto(Fortune Global 500) solve the pain points of Kafka elasticity in the V2X scenario](https://www.automq.com/blog/automq-help-geely-auto-solve-the-pain-points-of-kafka-elasticity-in-the-v2x-scenario)
- [How Asia’s Quora Zhihu uses AutoMQ to reduce Kafka cost and maintenance complexity](https://www.automq.com/blog/how-asias-quora-zhihu-use-automq-to-reduce-kafka-cost-and-maintenance-complexity)
- [XPENG Motors Reduces Costs by 50%+ by Replacing Kafka with AutoMQ](https://www.automq.com/blog/xpeng-motors-reduces-costs-by-50-by-replacing-kafka-with-automq)
- [Asia's GOAT, Poizon uses AutoMQ Kafka to build observability platform for massive data(30 GB/s)](https://www.automq.com/blog/asiax27s-goat-poizon-uses-automq-kafka-to-build-a-new-generation-observability-platform-for-massive-data)
- [AutoMQ Helps CaoCao Mobility Address Kafka Scalability During Holidays](https://www.automq.com/blog/automq-helps-caocao-mobility-address-kafka-scalability-issues-during-mid-autumn-and-national-day)

## 🗞️ Newest Feature
Table Topic feature for unified stream and data analysis, which now supports the S3 table feature announced at the 2024 re:Invent. [Learn more](https://www.automq.com/blog/automq-table-topic-seamless-integration-with-s3-tables-and-iceberg). 
![image](https://github.com/user-attachments/assets/6b2a514a-cc3e-442e-84f6-d953206865e0)


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
    <td>Guaranteed by cloud storage services[2]</td>
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

> [2] AutoMQ's flexible architecture can utilize the durability of various cloud storage services like S3, Regional EBS, and EFS, all offering multi-AZ durability.

## 🔶 Why AutoMQ

- **Cost effective**: The first true cloud-native streaming storage system, designed for optimal cost and efficiency on the cloud. Refer to [this report](https://docs.automq.com/docs/automq-opensource/EV6mwoC95ihwRckMsUKcppnqnJb) to see how we cut Apache Kafka billing by 90% on the cloud.
- **High Reliability**: Leverage cloud-shared storage services to achieve zero RPO, RTO in seconds and 99.999999999% durability.
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

![image](./docs/images/automq_simple_arch.png)

AutoMQ's Shared Storage architecture revolutionizes the storage layer of Apache Kafka by offloading data to cloud storage, thereby rendering the Broker stateless. This architecture incorporates both WAL (Write-Ahead Logging) storage and object storage, storing all data in object storage in near real-time.

In this setup:

- Object storage is the primary data repository, providing a flexible, cost-effective, and scalable storage solution.
- AutoMQ introduces a WAL storage layer to counter the high latency and low IOPS associated with Object storage, thereby improving data write efficiency and lowering IOPS usage.
- The WAL storage layer is adaptable, allowing for the selection of various storage services across different cloud providers to cater to diverse durability and performance needs. Azure Zone-redundant Disk, GCP Regional Persistent Disk, and Alibaba Cloud Regional ESSD are ideal for ensuring multi-AZ durability. For cost-effective solutions on AWS with relaxed latency scenarios, S3 can serve as WAL. Additionally, AWS EFS/FSx can balance latency and cost for critical workloads when used as WAL.

AutoMQ has developed a shared streaming storage library, S3Stream, which encapsulates these storage modules. By replacing the native Apache Kafka® Log storage with S3Stream, the entire Broker node becomes entirely stateless. This transformation significantly streamlines operations such as second-level partition reassignment, automatic scaling, and traffic self-balancing. To facilitate this, AutoMQ has integrated Controller components like Auto Scaling and Auto Balancing within its kernel, which oversee cluster scaling operations and traffic rebalancing, respectively. Please refer to [here](https://docs.automq.com/automq/architecture/overview) for more architecture details.

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
- [Try AutoMQ on Alibaba Cloud Marketplace (Two Weeks Free Trial)](https://market.aliyun.com/products/55530001/cmgj00065841.html)
- [Try AutoMQ on AWS Marketplace (Two Weeks Free Trial)](https://docs.automq.com/automq-cloud/getting-started/install-byoc-environment/aws/install-env-from-marketplace)

## 💬 Community
You can join the following groups or channels to discuss or ask questions about AutoMQ:
- Ask questions or report a bug by [GitHub Issues](https://github.com/AutoMQ/automq/issues)
- Discuss about AutoMQ or Kafka by [Slack](https://join.slack.com/t/automq/shared_invite/zt-29h17vye9-thf31ebIVL9oXuRdACnOIA) or [Wechat Group](docs/images/automq-wechat.png)


## 👥 How to contribute
If you've found a problem with AutoMQ, please open a [GitHub Issues](https://github.com/AutoMQ/automq/issues).
To contribute to AutoMQ please see [Code of Conduct](CODE_OF_CONDUCT.md) and [Contributing Guide](CONTRIBUTING_GUIDE.md).
We have a list of [good first issues](https://github.com/AutoMQ/automq/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) that help you to get started, gain experience, and get familiar with our contribution process. To claim one, simply reply with 'pick up' in the issue and the AutoMQ maintainers will assign the issue to you. If you have any questions about the 'good first issue' please feel free to ask. We will do our best to clarify any doubts you may have.

## 👍 AutoMQ Business Edition
The business edition of AutoMQ provides a powerful and easy-to-use control plane to help you manage clusters effortlessly. Meanwhile, the control plane is more powerful in terms of availability and observability compared to the community edition.

> You can check the difference between the community and business editions [here](https://www.automq.com/product).


<b>Watch the following video and refer to our [docs](https://docs.automq.com/automq-cloud/getting-started/install-byoc-environment/aws/install-env-via-terraform-module) to see how to deploy AutoMQ Business Edition with 2 weeks free license for PoC.</b>

<b> ⬇️ ⬇️ ⬇️ </b>

[![Deploy AutoMQ Business Edition with Terraform](https://img.youtube.com/vi/O40zp81x97w/0.jpg)](https://www.youtube.com/watch?v=O40zp81x97w)



### Free trial of AutoMQ Business Edition
To allow users to experience the capabilities of the AutoMQ business edition without any barriers, click [here](https://www.automq.com/quick-start#Cloud?utm_source=github_automq_cloud) to apply for a no-obligation cluster trial, and note `AutoMQ Cloud Free Trial` in the message input box. We will immediately initialize an AutoMQ Cloud control panel for you soon in the cloud and give you the address of the control panel. Then, you can use the control panel to create a AutoMQ cluster or perform operations like scale in/out. 

No need to bind a credit card, no cost at all. We look forward to receiving valuable feedback from you to make our product better. If you want to proceed with a formal POC, you can also contact us through [Contact Us](https://www.automq.com/contact). We will further support your official POC.

## 🐱 The relationship with Apache Kafka

AutoMQ is a fork of the open-source [Apache Kafka](https://github.com/apache/kafka). Based on the Apache Kafka codebase, we found an aspect at the LogSegment level, and replaced Kafka's storage layer with our self-developed cloud-native stream storage engine, [S3Stream](https://github.com/AutoMQ/automq/tree/main/s3stream). This engine can provide customers with high-performance, low-cost, and unlimited stream storage capabilities based on cloud storage like EBS WAL and S3. As such, AutoMQ completely retains the code of Kafka's computing layer and is 100% fully compatible with Apache Kafka. We appreciate the work done by the Apache Kafka community and will continue to embrace the Kafka community.

## 🙋 Contact Us
Want to learn more, [Talk with our product experts](https://www.automq.com/contact).
