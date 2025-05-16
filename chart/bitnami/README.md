# AutoMQ

[AutoMQ](https://www.automq.com/) is a cloud-native alternative to Kafka by decoupling durability to cloud storage services like S3. 10x Cost-Effective. No Cross-AZ Traffic Cost. Autoscale in seconds. Single-digit ms latency.
This Helm chart simplifies the deployment of AutoMQ into your Kubernetes cluster using the Software model.

## Prerequisites
### Install Helm chart
Install Helm chart and version v3.8.0+
[Helm chart quickstart](https://helm.sh/zh/docs/intro/quickstart/)
```shell
helm version
```
### Using the Bitnami Helm repository
AutoMQ is fully compatible with Bitnami's Helm Charts, so you can customize your AutoMQ Kubernetes cluster based on the relevant values.yaml of Bitnami.
[Bitnami Helm Charts](https://github.com/bitnami/charts)

## Quickstart
### Setup a Kubernetes Cluster
The quickest way to set up a Kubernetes cluster to install Bitnami Charts is by following the "Bitnami Get Started" guides for the different services:

[Get Started with Bitnami Charts using the Amazon Elastic Container Service for Kubernetes (EKS)](https://docs.bitnami.com/kubernetes/get-started-eks/)


### Installing the AutoMQ with Bitnami Chart

As an alternative to supplying the configuration parameters as arguments, you can create a supplemental YAML file containing your specific config parameters. Any parameters not specified in this file will default to those set in [values.yaml](values.yaml).

1. Create an empty `automq-values.yaml` file
2. Edit the file with your specific parameters:

You can refer to the [demo-values.yaml](/chart/bitnami/demo-values.yaml)  based on the bitnami [values.yaml](https://github.com/bitnami/charts/blob/main/bitnami/kafka/values.yaml)
we provided for deploying AutoMQ on AWS across 3 Availability Zones using m7g.xlarge instances (4 vCPUs, 16GB Mem, 156MiB/s network bandwidth).

3. Install or upgrade the AutoMQ Helm chart using your custom yaml file:

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
