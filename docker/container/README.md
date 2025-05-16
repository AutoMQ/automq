# AutoMQ

[AutoMQ](https://www.automq.com/) is a cloud-native alternative to Kafka by decoupling durability to cloud storage services like S3. 10x Cost-Effective. No Cross-AZ Traffic Cost. Autoscale in seconds. Single-digit ms latency.
This Helm chart simplifies the deployment of AutoMQ into your Kubernetes cluster using the Software model.

## Prerequisites
### Install Helm chart
Install Helm chart and version v3.6 or later.
[Helm chart quickstart](https://helm.sh/zh/docs/intro/quickstart/)
```shell
helm version
```
### Using the Bitnami Helm repository
AutoMQ is fully compatible with Bitnami's Helm Charts, so you can customize your AutoMQ Kubernetes cluster based on the relevant values.yaml of Bitnami.

## Quickstart

By default, the AutoMQ deployment runs as a ClusterIP Service with auto-scaling and authentication disabled and requires additional steps to grant the pods permission to access object storage.

### Installing the AutoMQ Chart

As an alternative to supplying the configuration parameters as arguments, you can create a supplemental YAML file containing your specific config parameters. Any parameters not specified in this file will default to those set in [values.yaml](values.yaml).

1. Create an empty `automq-values.yaml` file
2. Edit the file with your specific global parameters:

```yaml
# Bucket URI Pattern: 0@s3://$bucket?region=$region&endpoint=$endpoint
# Bucket URI Example:
# AWS : 0@s3://xxx_bucket?region=us-east-1
# AWS-CN: 0@s3://xxx_bucket?region=cn-northwest-1&endpoint=https://s3.amazonaws.com.cn&authType=instance&role=<role-id>
# ALIYUN: 0@s3://xxx_bucket?region=oss-cn-shanghai&endpoint=https://oss-cn-shanghai.aliyuncs.com&authType=instance&role=<role-id>
# TENCENT: 0@s3://xxx_bucket?region=ap-beijing&endpoint=https://cos.ap-beijing.myqcloud.com&authType=instance&role=<role-id>
# OCI: 0@s3://xxx_bucket?region=us-ashburn-1&endpoint=https://xxx_namespace.compat.objectstorage.us-ashburn-1.oraclecloud.com&authType=instance&role=<role-id>&pathStyle=true
# AZURE: 0@s3://xxx_bucket?region=us-east-1&endpoint=https://xxx_storage-account.blob.core.windows.net&authType=instance&role=<role-id>
extraConfig: |
    elasticstream.enable=true
    autobalancer.client.auth.sasl.mechanism=PLAIN
    autobalancer.client.auth.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="inter_broker_user" password="interbroker-password-placeholder" user_inter_broker_user="interbroker-password-placeholder";
    autobalancer.client.auth.security.protocol=SASL_PLAINTEXT
    autobalancer.client.listener.name=INTERNAL
    s3.ops.buckets=1@s3://<ops-bucket>?region=<region>&endpoint=<endpoint>
    s3.data.buckets=0@s3://<data-bucket>?region=<region>&endpoint=<endpoint>
    s3.wal.path=0@s3://<data-bucket>?region=<region>&endpoint=<endpoint>
```

3. Edit the file with your specific controller/broker parameters:
```yaml
controller:
  annotations:
    your-annotation-key: your-annotation-value
  resources:
    requests:
      cpu: "<your-cpu-request>"
      memory: "<your-memory-request>"
    limits:
      cpu: "<your-cpu-limit>"
      memory: "<your-memory-limit>"
  persistence:
    metadata:
      storageClass: "<your-storage-class>"
    wal:
      storageClass: "<your-storage-class>"
  env:
    - name: "KAFKA_HEAP_OPTS"
      value: "<your-heap-opts>"

```

```yaml
broker:
  replicas: 1
  annotations:
    your-annotation-key: your-annotation-value
  resources:
    requests:
      cpu: "<your-cpu-request>"
      memory: "<your-memory-request>"
    limits:
      cpu: "<your-cpu-limit>"
      memory: "<your-memory-limit>"
  persistence:
    wal:
      storageClass: "<your-storage-class>"
  env:
    - name: "KAFKA_HEAP_OPTS"
      value: "<your-heap-opts>"

```

4. Install or upgrade the AutoMQ Helm chart using your custom yaml file:

```shell
helm install my-release oci://registry-1.docker.io/bitnamicharts/<chart> -f <your-custom-values.yaml> --namespace <namespace> --create-namespace
```

### Upgrading

To upgrade the deployment:

```shell
helm repo update
helm upgrade my-release oci://registry-1.docker.io/bitnamicharts/<chart> -f <your-custom-values.yaml> --namespace <namespace> --create-namespace
```

### Uninstalling the Chart

To uninstall/delete the deployment:

```shell
helm uninstall my-release --namespace <namespace>
```

This command removes all the Kubernetes components associated with the chart and deletes the release.
