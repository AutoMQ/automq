\# AutoMQ Local Development Guide for Windows



This guide outlines the steps to set up a local development environment for AutoMQ on Windows using Docker and LocalStack.



\## Prerequisites



Ensure you have the following installed:

1\.  \*\*Java 17 (LTS)\*\*: Run `java -version` to verify.

2\.  \*\*Docker Desktop for Windows\*\*: Required to run LocalStack.

3\.  \*\*AWS CLI\*\*: Required to configure the mock S3 bucket.



---



\## 1. Troubleshooting Docker Setup (Important)



If you encounter errors starting Docker Desktop, check the following:



\### A. Enable Virtualization

If Docker fails to start, ensure \*\*Virtualization\*\* is enabled in your BIOS.

1\.  Open Task Manager (`Ctrl + Shift + Esc`) -> Performance -> CPU.

2\.  Check if "Virtualization" is \*\*Enabled\*\*.

3\.  If Disabled, restart your computer, enter BIOS (usually `F2`, `F10`, or `Del`), and enable \*\*Intel Virtual Technology\*\* or \*\*AMD SVM\*\*.



\### B. Update WSL 2

If you see a "WSL kernel version too low" error:

1\.  Open PowerShell as Administrator.

2\.  Run: `wsl --update`

3\.  Restart Docker Desktop.



---



\## 2. Setting up LocalStack (Mock Cloud)



AutoMQ requires an S3-compatible storage layer. We use LocalStack for this.



1\.  \*\*Start LocalStack Container:\*\*

&nbsp;   Open a PowerShell terminal and run:

&nbsp;   ```powershell

&nbsp;   docker run --rm -it -p 4566:4566 -p 4510-4559:4510-4559 localstack/localstack

&nbsp;   ```

&nbsp;   \*Keep this window open.\*



2\.  \*\*Configure AWS CLI (Dummy Credentials):\*\*

&nbsp;   Open a \*new\* PowerShell window and run:

&nbsp;   ```powershell

&nbsp;   aws configure

&nbsp;   ```

&nbsp;   - AWS Access Key ID: `test`

&nbsp;   - AWS Secret Access Key: `test`

&nbsp;   - Default region name: `us-east-1`

&nbsp;   - Default output format: `json`



3\.  \*\*Create S3 Bucket:\*\*

&nbsp;   Run the following command to create a bucket named `ko3`:

&nbsp;   ```powershell

&nbsp;   aws s3api create-bucket --bucket ko3 --region us-east-1 --endpoint-url=\[http://127.0.0.1:4566](http://127.0.0.1:4566)

&nbsp;   ```



---



\## 3. Configuration \& Running AutoMQ



\### A. Configure Server Properties

Open `config/kraft/server.properties` and ensure the S3 settings match your LocalStack setup.



\*\*Note on Region Mismatch:\*\*

If you encounter an error like `Authorization header is malformed; expecting 'us-east-2'`, update the region in the config file to match what LocalStack expects.



```properties

\# Updated settings for LocalStack

s3.endpoint=\[http://127.0.0.1:4566](http://127.0.0.1:4566)

s3.region=us-east-1 

s3.bucket=ko3

