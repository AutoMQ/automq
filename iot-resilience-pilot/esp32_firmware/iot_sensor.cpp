/**
 * IoT Sensor Firmware - ESP32 Reference Implementation
 * 
 * Demonstrates "Store-and-Forward" pattern for IoT resilience:
 * - Normal mode: Read sensors, send UDP packets every 1 second
 * - Offline mode: Buffer data to LittleFS when WiFi disconnected
 * - Flush mode: Blast buffered data at high speed on reconnection
 * 
 * Compatible with the IoT Ingestion Gateway in this project.
 * 
 * Platform: ESP32 (Arduino Framework)
 * Requires: LittleFS, WiFi libraries
 */

#include <Arduino.h>
#include <WiFi.h>
#include <WiFiUdp.h>
#include <LittleFS.h>
#include <ArduinoJson.h>

// ============================================================================
// Configuration - Update these for your environment
// ============================================================================

// WiFi credentials
const char* WIFI_SSID = "YOUR_WIFI_SSID";
const char* WIFI_PASSWORD = "YOUR_WIFI_PASSWORD";

// Gateway configuration
const char* GATEWAY_HOST = "192.168.1.100";  // Update with gateway IP
const uint16_t GATEWAY_PORT = 5000;

// Device identification (should be unique per device)
const char* DEVICE_ID = "ESP32_00001";

// Timing configuration
const uint32_t NORMAL_INTERVAL_MS = 1000;   // 1 second between readings
const uint32_t FLUSH_INTERVAL_MS = 30;       // 30ms between flush packets
const uint32_t WIFI_RECONNECT_DELAY_MS = 5000;

// Buffer file
const char* BUFFER_FILE = "/buffer.bin";
const size_t MAX_BUFFER_SIZE = 100000;  // Max 100KB buffer

// ============================================================================
// Global Variables
// ============================================================================

WiFiUDP udp;
uint32_t sequence = 0;
bool wasConnected = false;

// Operating modes
enum class Mode {
    NORMAL,     // Connected, sending live data
    OFFLINE,    // Disconnected, buffering to LittleFS
    FLUSHING    // Reconnected, blasting buffered data
};

Mode currentMode = Mode::OFFLINE;

// ============================================================================
// Sensor Simulation (Replace with actual sensor readings)
// ============================================================================

struct SensorData {
    float temperature;
    float humidity;
    float pressure;
    float battery;
};

SensorData readSensors() {
    // Simulate sensor readings - replace with actual sensor code
    // e.g., DHT22, BME280, etc.
    SensorData data;
    data.temperature = 20.0f + random(-500, 1500) / 100.0f;
    data.humidity = 30.0f + random(0, 5000) / 100.0f;
    data.pressure = 980.0f + random(0, 4000) / 100.0f;
    data.battery = 3.3f + random(0, 90) / 100.0f;
    return data;
}

// ============================================================================
// Packet Creation
// ============================================================================

/**
 * Create a binary packet with structured header
 * Format: [2B magic 0xAA55][16B device_id][4B sequence][JSON payload]
 */
size_t createPacket(uint8_t* buffer, size_t bufferSize, const SensorData& data) {
    if (bufferSize < 24) return 0;
    
    // Magic bytes
    buffer[0] = 0xAA;
    buffer[1] = 0x55;
    
    // Device ID (16 bytes, null-padded)
    memset(&buffer[2], 0, 16);
    strncpy((char*)&buffer[2], DEVICE_ID, 15);
    
    // Sequence number (big-endian)
    sequence++;
    buffer[18] = (sequence >> 24) & 0xFF;
    buffer[19] = (sequence >> 16) & 0xFF;
    buffer[20] = (sequence >> 8) & 0xFF;
    buffer[21] = sequence & 0xFF;
    
    // JSON payload
    StaticJsonDocument<256> doc;
    doc["ts"] = millis() / 1000.0;
    
    JsonObject sensorObj = doc.createNestedObject("data");
    sensorObj["temperature"] = data.temperature;
    sensorObj["humidity"] = data.humidity;
    sensorObj["pressure"] = data.pressure;
    sensorObj["battery"] = data.battery;
    
    size_t payloadLen = serializeJson(doc, (char*)&buffer[22], bufferSize - 22);
    
    return 22 + payloadLen;
}

// ============================================================================
// UDP Transmission
// ============================================================================

bool sendPacket(const uint8_t* packet, size_t length) {
    if (WiFi.status() != WL_CONNECTED) {
        return false;
    }
    
    udp.beginPacket(GATEWAY_HOST, GATEWAY_PORT);
    size_t written = udp.write(packet, length);
    int result = udp.endPacket();
    
    return (result == 1 && written == length);
}

// ============================================================================
// LittleFS Buffer Management
// ============================================================================

bool initFileSystem() {
    if (!LittleFS.begin(true)) {  // true = format on fail
        Serial.println("ERROR: Failed to mount LittleFS");
        return false;
    }
    Serial.println("LittleFS mounted successfully");
    return true;
}

size_t getBufferSize() {
    if (!LittleFS.exists(BUFFER_FILE)) {
        return 0;
    }
    File file = LittleFS.open(BUFFER_FILE, "r");
    if (!file) return 0;
    size_t size = file.size();
    file.close();
    return size;
}

bool bufferPacket(const uint8_t* packet, size_t length) {
    // Check max buffer size
    if (getBufferSize() + length + 2 > MAX_BUFFER_SIZE) {
        Serial.println("WARNING: Buffer full, dropping packet");
        return false;
    }
    
    File file = LittleFS.open(BUFFER_FILE, "a");
    if (!file) {
        Serial.println("ERROR: Failed to open buffer file for writing");
        return false;
    }
    
    // Write length prefix (2 bytes, little-endian)
    uint8_t lenBytes[2] = {
        (uint8_t)(length & 0xFF),
        (uint8_t)((length >> 8) & 0xFF)
    };
    file.write(lenBytes, 2);
    
    // Write packet data
    file.write(packet, length);
    file.close();
    
    return true;
}

/**
 * Read and send all buffered packets
 * Returns the number of packets sent
 */
uint32_t flushBuffer() {
    if (!LittleFS.exists(BUFFER_FILE)) {
        return 0;
    }
    
    File file = LittleFS.open(BUFFER_FILE, "r");
    if (!file) {
        Serial.println("ERROR: Failed to open buffer file for reading");
        return 0;
    }
    
    uint32_t packetsSent = 0;
    uint8_t packet[512];
    
    Serial.println("FLUSH: Starting buffer flush...");
    
    while (file.available() >= 2) {
        // Check if we're still connected
        if (WiFi.status() != WL_CONNECTED) {
            Serial.println("FLUSH: Lost connection during flush, pausing");
            file.close();
            return packetsSent;
        }
        
        // Read length prefix
        uint8_t lenBytes[2];
        if (file.read(lenBytes, 2) != 2) break;
        
        uint16_t length = lenBytes[0] | (lenBytes[1] << 8);
        
        if (length > sizeof(packet)) {
            Serial.printf("ERROR: Invalid packet length: %d\n", length);
            break;
        }
        
        // Read packet data
        if (file.read(packet, length) != length) break;
        
        // Send packet
        if (sendPacket(packet, length)) {
            packetsSent++;
        } else {
            Serial.printf("WARNING: Failed to send buffered packet %d\n", packetsSent);
        }
        
        // Rate limit flush to avoid overwhelming gateway
        delay(FLUSH_INTERVAL_MS);
    }
    
    file.close();
    
    // Clear buffer file after successful flush
    LittleFS.remove(BUFFER_FILE);
    
    Serial.printf("FLUSH: Complete! Sent %d buffered packets\n", packetsSent);
    return packetsSent;
}

// ============================================================================
// WiFi Management
// ============================================================================

void connectWiFi() {
    Serial.printf("Connecting to WiFi: %s\n", WIFI_SSID);
    
    WiFi.mode(WIFI_STA);
    WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
    
    uint32_t startTime = millis();
    while (WiFi.status() != WL_CONNECTED && millis() - startTime < 10000) {
        delay(500);
        Serial.print(".");
    }
    
    if (WiFi.status() == WL_CONNECTED) {
        Serial.printf("\nWiFi connected! IP: %s\n", WiFi.localIP().toString().c_str());
    } else {
        Serial.println("\nWiFi connection failed, will retry...");
    }
}

// ============================================================================
// Main Setup & Loop
// ============================================================================

void setup() {
    Serial.begin(115200);
    delay(1000);
    
    Serial.println("\n========================================");
    Serial.println("IoT Sensor - Store-and-Forward Demo");
    Serial.printf("Device ID: %s\n", DEVICE_ID);
    Serial.println("========================================\n");
    
    // Initialize file system for buffering
    if (!initFileSystem()) {
        Serial.println("FATAL: Cannot continue without filesystem");
        while (1) delay(1000);
    }
    
    // Check if we have buffered data from previous session
    size_t existingBuffer = getBufferSize();
    if (existingBuffer > 0) {
        Serial.printf("Found %d bytes of buffered data from previous session\n", existingBuffer);
    }
    
    // Connect to WiFi
    connectWiFi();
    
    // Initialize random seed for sensor simulation
    randomSeed(esp_random());
}

void loop() {
    static uint32_t lastSensorRead = 0;
    static uint32_t lastWiFiCheck = 0;
    
    uint32_t now = millis();
    
    // Check WiFi status periodically
    if (now - lastWiFiCheck > 1000) {
        lastWiFiCheck = now;
        
        bool isConnected = (WiFi.status() == WL_CONNECTED);
        
        // Detect connection state change
        if (isConnected && !wasConnected) {
            // Just reconnected!
            Serial.println("\n*** WiFi RECONNECTED! ***");
            currentMode = Mode::FLUSHING;
        } else if (!isConnected && wasConnected) {
            // Just disconnected
            Serial.println("\n*** WiFi DISCONNECTED! Entering offline mode ***");
            currentMode = Mode::OFFLINE;
        }
        
        wasConnected = isConnected;
    }
    
    // Handle current mode
    switch (currentMode) {
        case Mode::FLUSHING: {
            // Flush all buffered data
            uint32_t flushed = flushBuffer();
            Serial.printf("Flushed %d buffered packets, entering normal mode\n", flushed);
            currentMode = Mode::NORMAL;
            break;
        }
        
        case Mode::NORMAL:
        case Mode::OFFLINE: {
            // Read sensors at regular interval
            if (now - lastSensorRead >= NORMAL_INTERVAL_MS) {
                lastSensorRead = now;
                
                // Read sensor data
                SensorData data = readSensors();
                
                // Create packet
                uint8_t packet[256];
                size_t packetLen = createPacket(packet, sizeof(packet), data);
                
                if (currentMode == Mode::NORMAL) {
                    // Try to send directly
                    if (sendPacket(packet, packetLen)) {
                        Serial.printf("[SEND] Seq: %d, Temp: %.1f°C\n", 
                                     sequence, data.temperature);
                    } else {
                        // Send failed, buffer it
                        bufferPacket(packet, packetLen);
                        Serial.printf("[BUFFER] Seq: %d (send failed)\n", sequence);
                    }
                } else {
                    // Offline mode - buffer to LittleFS
                    if (bufferPacket(packet, packetLen)) {
                        size_t bufSize = getBufferSize();
                        Serial.printf("[BUFFER] Seq: %d, Buffer: %d bytes\n", 
                                     sequence, bufSize);
                    }
                }
            }
            
            // If offline, periodically try to reconnect
            if (currentMode == Mode::OFFLINE && now - lastWiFiCheck > WIFI_RECONNECT_DELAY_MS) {
                Serial.println("Attempting WiFi reconnection...");
                connectWiFi();
            }
            break;
        }
    }
    
    // Small delay to prevent tight loop
    delay(10);
}
