# 🚀 Getting Started: Beginner's Guide

You don't need to be a programmer to run KLAIS! If you have **Docker** installed on your computer, you can see KLAIS in action with just a few clicks or simple commands.

---

## 1. Prerequisites
- **Docker & Docker Compose:** This is like a "shipping container" system for software. It lets you run KLAIS without installing all the complicated "guts." 
  - [Download Docker Desktop here](https://www.docker.com/products/docker-desktop/) if you don't have it.

---

## 2. Running KLAIS
1.  **Open a Terminal:** On Windows, search for "PowerShell" or "Command Prompt."
2.  **Navigate to this folder:** Use the `cd` command to go to where you downloaded KLAIS.
3.  **Start it up:** Type the following and press Enter:
    ```bash
    docker-compose up -d
    ```
    *This might take a minute the first time. It's downloading the "factory" parts (Kafka, Prometheus, and KLAIS).*

---

## 3. How to See it Working
Once it's running, you can "look inside" using your web browser:

- **📊 The Dashboard (Grafana):** Go to `http://localhost:3000`. You'll see graphs of data moving through the system.
- **📈 The Metrics (Prometheus):** Go to `http://localhost:9090` to see the raw numbers the "Brain" is watching.
- **🏥 System Status:** Go to `http://localhost:8080/health`. If it says `"status": "ok"`, the gatekeeper is on duty!

---

## 4. Sending "Fake" Data (Test)
To see KLAIS actually block or pass data, you can use a simple tool to send a test message. If you have `curl` installed (standard on most computers now), try this in your terminal:

```bash
curl http://localhost:8080/stats
```
*This will show you how many "visitors" (packets) have been seen so far!*

---

## 5. Stopping
When you're done, just type:
```bash
docker-compose down
```

---

**Common Problems:**
- **"Port already in use":** This means another program is using the "doorway" KLAIS needs. Close other apps like Skype or older web servers.
- **"Docker not found":** Make sure Docker Desktop is actually running (look for the whale icon in your taskbar).
