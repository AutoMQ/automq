<h1 align="center">
AutoMQ: A Cloud-Native fork of Kafka by separating storage to S3
</h1>
<h3 align="center">

</h3>

![GitHub release (with filter)](https://img.shields.io/github/v/release/AutoMQ/automq-for-kafka)
![GitHub pull requests](https://img.shields.io/github/issues-pr/AutoMQ/automq-for-kafka)
![GitHub closed pull requests](https://img.shields.io/github/issues-pr-closed/AutoMQ/automq-for-kafka)
![](https://img.shields.io/badge/Java%20-%20JDK17-green)

---
[![](https://img.shields.io/badge/Official%20Website-20B2AA)](https://www.automq.com)
&nbsp;
[![](https://img.shields.io/badge/Document-blue)](https://docs.automq.com/docs/automq-s3kafka/YUzOwI7AgiNIgDk1GJAcu6Uanog)
&nbsp;
[![](https://img.shields.io/badge/AutoMQ%20vs.%20Kafka(Cost)-yellow)]([https://www.automq.com](https://www.automq.com/blog/automq-vs-apache-kafka-a-real-aws-cloud-bill-comparison))
&nbsp;
[![](https://img.shields.io/badge/AutoMQ%20vs.%20Kafka(Performance)-orange)]([https://www.automq.com](https://docs.automq.com/docs/automq-s3kafka/CYxlwqDBHitThCkxSl2cePxrnBc))
&nbsp;
[![Twitter URL](https://img.shields.io/twitter/follow/AutoMQ)](https://twitter.com/intent/follow?screen_name=AutoMQ_Lab)</a>
&nbsp;
<a href="docs/images/automq-wechat.png" target="_blank"><img src="https://img.shields.io/badge/- Wechat -red?style=social&logo=discourse" height=20></a>
&nbsp;

---

<img src="https://img.shields.io/badge/aws%20cloud-supported-lightgreen?style=for-the-badge&logo=amazonaws" height="18"><img src="https://img.shields.io/badge/google%20cloud-todo-lightyellow?style=for-the-badge&logo=googlecloud" height="18"><img src="https://img.shields.io/badge/Azure%20cloud-todo-lightyellow?style=for-the-badge&logo=microsoftazure" height="18"><img src="https://img.shields.io/badge/aliyun%20cloud-supported-lightgreen?style=for-the-badge&logo=alibabacloud" height="18"><img src="https://img.shields.io/badge/huawei%20cloud-supported-lightgreen?style=for-the-badge&logo=huawei" height="18"><img src="https://img.shields.io/badge/baidu%20cloud-supported-lightgreen?style=for-the-badge&logo=baidu" height="18"><img src="https://img.shields.io/badge/tencent%20cloud-supported-lightgreen?style=for-the-badge&logo=tencentqq" height="18">


[//]: # ([![E2E_TEST]&#40;https://github.com/AutoMQ/automq-for-kafka/actions/workflows/nightly-e2e.yml/badge.svg&#41;]&#40;https://github.com/AutoMQ/automq-for-kafka/actions/workflows/nightly-e2e.yml&#41;)

## 🍵 AutoMQ vs Other Streaming Platforms


| Feature | AutoMQ | Apache Kafka | Confluent | Apache Pulsar | Redpanda | Warpstream |
|---|---|---|---|---|---|---|
| Apache Kafka Compatibility | Native Kafka | Native Kafka | Native Kafka | Non-Kafka | Kafka Protocol | Kafka Protocol |
| Source Code Availability | Yes | Yes | No | Yes | Yes | No |
| Stateless Broker | Yes | No | No | Yes | No | Yes |
| P99 Latency | Single-digit ms latency | Single-digit ms latency | Single-digit ms latency | Single-digit ms latency | Single-digit ms latency | [> 400ms](https://www.warpstream.com/blog/kafka-is-dead-long-live-kafka)	|
| Continuous Self-Balancing | Yes | No | Yes | Yes | Yes | Yes |
| Scale in/out | In seconds | In hours/days | In hours | In hours(scale-in);<br> In seconds(scale-out) | In hours | In seconds |
| Spot Instance Support | Yes | No | No | No | No | Yes |
| Partition Reassignment | In seconds | In hours/days | In hours | In seconds | In hours | In seconds |
| Component  | Broker<br> Controller | Broker<br>Controller<br>Zookeeper(Non-Kraft) | Broker<br>Controller<br>Zookeeper(Non-Kraft) | Broker<br>Controller<br>Zookeeper<br>Bookkeeper<br>Proxy | Broker<br>Controller | Agent<br>MetadataServer |

> Tips: Apache Kafka Compatibility's definition is comming from this [blog](https://www.kai-waehner.de/blog/2021/05/09/kafka-api-de-facto-standard-event-streaming-like-amazon-s3-object-storage/).


## 🔶Why AutoMQ

- **Cloud Native**: Built on cloud service. Every system design decision take cloud service's feature and billing items into consideration to offer best low-latency, scalable, reliable and cost-effective Kafka service on cloud.
- **High Reliability**: Leverage the features of cloud service to offer RPO of 0 and RTO in seconds. 
  - AWS: Use S3 express one zone and S3 to offer AZ level disaster recovery. 
  - GCP: Use regional SSD and cloud storage to offer AZ level disaster recovery. 
  - Azure: Use zone-redundant storage and blob storage to offer AZ level disaster recovery.
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

![image](./docs/images/automq-kafka-compare.png)


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
