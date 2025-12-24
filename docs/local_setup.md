# Local Setup Guide for New Contributors

Welcome to the AutoMQ community! This guide will help you set up your local development environment to start contributing.

## Prerequisites

Before you begin, ensure you have the following installed:

*   **Java Development Kit (JDK)**: Version 17 is required. 
    *   *Note*: Newer versions (e.g., JDK 21/23) are generally compatible but 17 is the baseline.
*   **Git**: For version control.
*   **IntelliJ IDEA** (Recommended): The project includes configuration files optimized for IntelliJ.

## step-by-step Setup

### 1. Fork and Clone the Repository

First, fork the [AutoMQ repository](https://github.com/automq/automq) to your own GitHub account. Then, clone it locally:

```bash
git clone https://github.com/YOUR_USERNAME/automq.git
cd automq
```

### 2. Verify Your Environment

Ensure your `JAVA_HOME` is correctly set. AutoMQ uses Gradle, which relies on this environment variable.

On Windows (PowerShell):
```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-17" # Adjust path to your installation
./gradlew --version
```

On macOS/Linux:
```bash
export JAVA_HOME=/path/to/jdk-17
./gradlew --version
```

### 3. Build the Project

To verify that everything is configured correctly, run the build command. You can skip tests for the initial build to save time:

```bash
./gradlew jar -x test
```

If the build shows `BUILD SUCCESSFUL`, you are ready to go!

### 4. Import into IntelliJ IDEA

1.  Open IntelliJ IDEA.
2.  Select **Open** and choose the `automq` directory.
3.  The project contains a `build.gradle` file, so IntelliJ should recognize it as a Gradle project automatically.
4.  Run `gradlew spotlessApply` to format the code according to the project's style guidelines.

## Troubleshooting

### "Invalid JAVA_HOME" or "Unsupported class file major version"
If you see an error like `ERROR: JAVA_HOME is set to an invalid directory` or **`Unsupported class file major version 67`** (which indicates Java 23), it means your environment is using an incompatible Java version.
*   This project strictly requires **JDK 17**.
*   Check your `JAVA_HOME` by running `echo $env:JAVA_HOME` (Windows) or `echo $JAVA_HOME` (Linux/Mac).
*   Find your actual Java installation path (e.g., `C:\Program Files\Java\jdk-17`) and update the variable.

### Windows Encoding Issues
If you encounter `unmappable character for encoding windows-1252` errors during the build, you need to force the Java compiler to use UTF-8.
Run this command in PowerShell before building:
```powershell
$env:JAVA_TOOL_OPTIONS="-Dfile.encoding=UTF-8"
```

### Gradle Daemon Issues
If the build hangs or fails unexpectedly, try stopping the Gradle daemon:
```bash
./gradlew --stop
```
Then run the build again.

---
Happy coding! If you encounter any issues not listed here, please open an issue or reach out to the community.
