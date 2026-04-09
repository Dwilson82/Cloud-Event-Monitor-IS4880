Cloud Event Monitor (IS4880)
Overview

Cloud Event Monitor is an event-driven temperature monitoring system developed for the IS4880 capstone project. The system captures temperature readings from a Raspberry Pi (or simulated source), publishes them as events to Google Cloud Pub/Sub, processes them through a cloud-based consumer, stores them in a Cloud SQL database, and displays them through a real-time web dashboard.

The system demonstrates a complete end-to-end event pipeline, including event production, message delivery, duplicate handling, persistence, and visualization.

System Architecture

The system follows an event-driven architecture:

Producer (Raspberry Pi / Simulation)
        ↓
Google Cloud Pub/Sub
        ↓
Consumer (Cloud Run Service)
        ↓
Cloud SQL Database
        ↓
Web Dashboard (Flask + HTML/JS)

This architecture ensures:

Decoupled components
Scalable message processing
At-least-once delivery handling
Real-time monitoring and visualization
Features
Producer (Event Monitor)
Live temperature monitoring using DS18B20 sensor
Simulated temperature generation for testing
Configurable thresholds and alert behavior
Event publishing to Google Cloud Pub/Sub
Duplicate event simulation (configurable)
Structured message ID generation
Messaging (Pub/Sub)
Event-based communication between producer and consumer
At-least-once delivery model
Message attributes and payload handling
Consumer (Cloud Run)
Processes incoming Pub/Sub messages
Parses and validates event payloads
Handles duplicate messages using message_id
Stores processed events in Cloud SQL
Database (Cloud SQL)
Stores all events (including duplicates)
Tracks duplicate status using is_duplicate
Supports querying for dashboard display
Dashboard (Flask Web App)
Real-time event display
Duplicate highlighting in UI
Event filtering (all, duplicates, unique)
Metrics:
Total events
Duplicate count
Average temperature
Time-series chart visualization
System status display
Event Structure

Each event contains:

{
  "message_id": "sim-TEMP-AA05",
  "device_id": "rpi-sim",
  "mode": "sim",
  "temp_c": 36.5,
  "temp_f": 97.7,
  "timestamp_utc": "2026-03-26T16:12:35Z",
  "sequence": 5,
  "event_type": "TEMP_READING"
}

Additional fields may be included for threshold events:

TEMP_THRESHOLD_EXCEEDED
TEMP_THRESHOLD_RECOVERED
Duplicate Handling

The system follows an at-least-once delivery model, meaning duplicate messages may occur.

Duplicate handling is implemented in the consumer:

Messages are identified using message_id
All messages are stored (including duplicates)
Duplicate records are flagged using is_duplicate
The dashboard highlights duplicate events visually
Message ID Design

Message IDs follow a structured format:

[device]-[event_type]-[series]
Example: sim-TEMP-AA05

The series component increments sequentially and is persisted across runs. While the sequence has a finite range and may eventually cycle, it is sufficient for project scope. In production systems, a globally unique identifier (UUID) would be used to guarantee uniqueness.

File Structure
event_monitor_main.py   # Producer application (live + simulated modes)
config.json            # Configuration file (thresholds, IDs, settings)
sensor_readings.log    # Local event log file

app.py                 # Flask API and dashboard backend
templates/index.html   # Dashboard UI

How It Works
Producer
Generates temperature readings (live or simulated)
Builds event payloads
Publishes events to Pub/Sub
Consumer
Receives events from Pub/Sub via Cloud Run
Decodes and processes messages
Inserts records into Cloud SQL
Flags duplicates
Dashboard
Retrieves data from API endpoints:
/api/events
/api/metrics
/api/chart-data
Displays real-time system data and analytics
Requirements
Hardware (Optional for Live Mode)
Raspberry Pi
DS18B20 temperature sensor
1-Wire interface enabled
Software
Python 3
Flask
PyMySQL
Google Cloud SDK / Pub/Sub
Chart.js (frontend)
Running the System
Producer (Raspberry Pi or Local Simulation)
python event_monitor_main.py
Dashboard (Flask App)
python app.py

Access dashboard at:

https://dashboard-ui-693725813322.us-east1.run.app/


Project Purpose

This project demonstrates:

Event-driven system design
Cloud messaging using Pub/Sub
Distributed processing architecture
Duplicate message handling
Real-time monitoring and visualization
Future Enhancements
Global unique ID generation (UUID)
Advanced anomaly detection
Alert notifications
Historical data analytics
Multi-device monitoring
Author

Dustin Wilson
IS4880 Capstone Project
Cloud Event Monitor