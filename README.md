# Weather Data Pipeline (ETL) with Docker & Cron

A lightweight Data Engineering project that automates the extraction, transformation, and loading (ETL) of weather data using Python and containerization.

## 📌 Project Overview
This project captures real-time weather data from an API and stores it in a structured CSV format. It is designed to demonstrate core Data Engineering principles:
* **ETL Logic:** Fetching (Extract), processing (Transform), and saving (Load) data.
* **Containerization:** Packaging the application with **Docker** for environment consistency.
* **Data Persistence:** Using **Docker Volumes** to ensure data survives container restarts.
* **Automation:** Scheduling periodic runs using **Cron** jobs.

## 🚀 Technical Stack
* **Language:** Python 3.x
* **Libraries:** `requests`, `pandas`
* **Infrastructure:** Docker
* **Scheduling:** Linux Cron (Time-based job scheduler)

## 🛠️ Installation & Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/Bahna-Darius/data-engineering-weather.git
   cd weather_project
