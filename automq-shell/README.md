# AutoMQ CLI

## Build and run

```bash
# In the root directory
./gradlew releaseTarGz

# Extract the distribution, then run the CLI
tar -xzf core/build/distributions/kafka_*.tgz
cd kafka_*
./bin/automq-cli.sh -h
```

The `:automq-shell:jar` artifact is a thin JAR and is not intended to be run with `java -jar`.
