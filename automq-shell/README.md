# AutoMQ CLI

## Build

```bash
# In the root directory
./gradlew :automq-shell:jar -x test
java -jar automq-shell/build/libs/automq-shell-3.8.0-SNAPSHOT.jar -h

# Build native image
native-image -jar automq-shell/build/libs/automq-shell-3.8.0-SNAPSHOT.jar --initialize-at-build-time=org.slf4j.LoggerFactory
```