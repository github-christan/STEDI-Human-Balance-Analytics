# STEDI Data Lakehouse Project

This project implements a data lakehouse solution on AWS for the STEDI team, using AWS Glue, S3, Athena, Python, and Spark. The pipeline ingests raw IoT and customer data, transforms it into trusted and curated zones, and produces a machine-learning-ready dataset.

## Project Workflow
1. Landing Zone – Raw JSON data is stored in S3:
   - customer_landing
   - accelerometer_landing
   - step_trainer_landing

2. Glue Tables – External tables created for each landing zone:
   - customer_landing.sql
   - accelerometer_landing.sql
   - step_trainer_landing.sql

3. Trusted Zone – Data is filtered to include only customers who consented to share data:
   - customer_landing_to_trusted.py
   - accelerometer_landing_to_trusted.py
   - step_trainer_landing_to_trusted.py

4. Curated Zone – Refined data for analytics and ML:
   - customer_trusted_to_curated.py
   - machine_learning_curated.py

5. Athena Queries and Validation – Screenshots of query results are included for:
   - customer_landing.png
   - accelerometer_landing.png
   - step_trainer_landing.png
   - customer_trusted.png

## Row Count Validation
Expected row counts at each stage:

- Landing  
  - Customer: 956  
  - Accelerometer: 81,273  
  - Step Trainer: 28,680  

- Trusted  
  - Customer: 482  
  - Accelerometer: 40,981  
  - Step Trainer: 14,460  

- Curated  
  - Customer: 482  
  - Machine Learning: 43,681  

## Repository Contents
- SQL Scripts – Glue external table definitions  
- PySpark Scripts – AWS Glue ETL jobs for trusted and curated datasets  
- Screenshots – Athena query results for validation  
- Diagram – Workflow of the lakehouse solution  

## Technologies Used
- AWS Glue – ETL orchestration  
- AWS S3 – Data lake storage  
- AWS Athena – Querying and validation  
- PySpark – Transformations and joins  
