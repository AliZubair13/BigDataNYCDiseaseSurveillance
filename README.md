# BigDataNYCDiseaseSurveillance

Repository for Fall 2025 Big Data Final Project: NYC Disease Outbreak Surveillance

## Team Members
- Devak Somaraj (ds8095)
- Steven Granaturov (sg8002)
- Reetahan Mukhopadhyay (rm6609)
- Adhyayan Verma (av4159)
- Zubair Ali (zl5749)

## Project Overview

NYC Disease Outbreak Surveillance provides hyperlocal disease monitoring for New York City by integrating unofficial data sources with official public health reports to support early outbreak detection at the neighborhood level.

## System Architecture

<img width="944" height="401" alt="image" src="https://github.com/user-attachments/assets/26ab58e9-5187-409c-8001-22277552ffc1" />


## Quick Start

### Setup

1. Clone the repository and create virtual environment:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

2. Configure environment:
   ```bash
   cp .env.example .env
   # Edit .env with your credentials
   ```

3. Start all Docker containers (Kafka, Kafka UI, TimescaleDB):
   ```bash
   docker-compose up -d
   ```
   Wait a few seconds for all services to be ready.

4. Run the project:
   ```bash
   python run_project.py
   ```
   This will run the whole project in one shot (may need to wait for certain consumers to finish).
   You may also run the scrapers individually, the Spark consumers or individually or via the run_chained_project.py,
   and set up the Postgres/Timescale DB via the psql_db_client.py options and ChromaDB via the chromadb_client.py,
   then run the individual Spark analysis scripts in the analysis folder, and you can run the dashboard app in the
   dashboard folder as 
   ```bash
   streamlit run app_upgraded.py
   ```



## Results

   <img width="836" height="399" alt="image" src="https://github.com/user-attachments/assets/b65a9351-0fa4-4b31-9f55-044b4e17cd07" />

   <img width="826" height="472" alt="image" src="https://github.com/user-attachments/assets/19d2b290-c9c7-4326-9f43-1b85cb4f1a2a" />

   <img width="826" height="418" alt="image" src="https://github.com/user-attachments/assets/d3a73f76-1694-4868-ade9-d22377be0b9a" />

   <img width="524" height="515" alt="image" src="https://github.com/user-attachments/assets/8fed0455-168f-4b8b-bbd6-860537dfcdca" />





   
