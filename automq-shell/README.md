# AutoMQ CLI

## Build

```bash
# In the root directory
./gradlew :cli:jar -x test
java -jar cli/build/libs/cli-3.8.0-SNAPSHOT.jar -h

# Build native image
native-image -jar cli/build/libs/cli-3.8.0-SNAPSHOT.jar --initialize-at-build-time=org.slf4j.LoggerFactory
```