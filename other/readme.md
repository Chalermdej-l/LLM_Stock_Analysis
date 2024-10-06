# LLM Stock Analysis Setup Guide
This guide details the steps to reproduce this project, which is a comprehensive stock analysis system powered by Large Language Models (LLM). The project combines web scraping, natural language processing, and database management to provide intelligent stock market insights and analysis.

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [Initial Setup](#initial-setup)
3. [Infrastructure Setup](#infrastructure-setup)
4. [Docker Setup](#docker-setup)
5. [Using the Application](#using-the-application)
   - [1. Chat Bot Interface](#1-chat-bot-interface)
   - [2. Pipeline Execution](#2-pipeline-execution)
   - [3. Summary Generation](#3-summary-generation)
   - [4. LLM Detailed Analysis](#4-ask-llm-for-more-detail)
   - [5. RAG Database Integration](#5-rag-fetches-data-from-database)
   - [6. Chat History](#6-chat-history)
   - [7. Persistent Storage](#7-persistent-storage)
6. [Cleanup](#cleanup)

## Prerequisites
- Git
- Terraform
- Make
- Docker and Docker Compose

## Initial Setup
1. Clone the repository:
```bash
git clone https://github.com/Chalermdej-l/LLM_Stock_Analysis.git
```
Then navigate to the project directory:
```bash
cd LLM_Stock_Analysis/
```

2. Configure environment variables and update the [.env](/.env.example) file Groq API_KEY, Google Project_ID, Magic PW, USER :
   - Rename `.env.example` to `.env`
   - Update the necessary variables in:
     - `.env` file
       
![1](/image/chainlit/1.png)

- Also update the Google Project ID in the Terraform [environment](/infra/terraform.tfvars) configuration
     
![2](/image/reproduce/2.png)
## Infrastructure Setup
1. Install required tools:
   - Install Terraform
   - Install Make

2. Initialize and plan the infrastructure:
```bash
make infra-init
```
![3](/image/reproduce/3.png)
![4](/image/reproduce/4.png)
After you review the resource to create run to create the infra:
```bash
make infra-up
```
![5](/image/reproduce/5.png)
> Note: Infrastructure creation may take approximately 10 minutes to complete

This will create IAM account
![iam](/image/resource/iam.png)

Service Account
![sa](/image/resource/sa.png)

SQL database
![sql](/image/resource/sql.png)


3. Run the below command to output the service account key. This will create a new folder call key and put the service_account.key in the folder:
```bash
terraform -chdir=infra output -raw service_account_key | python code/decode_key.py --encode_key="$(cat)"
```
![6](/image/reproduce/6.png)
## Docker Setup
1. Prepare Docker environment:
   - Ensure `.env` file is properly configured
   - Rename `.env.example` if needed

2. Build Docker containers:
```bash
make docker-build
```
![7](/image/reproduce/7.png)
Then run the containers:
```bash
make docker-up
```
![8](/image/reproduce/8.png)
> Note: The init db is only use to create the table on the in the database first init instance of the docker
![docker](/image/resource/docker.png)
This will spawn two Docker containers.
![dk1](/image/resource/chainlit.png)
![dk2](/image/resource/proxy.png)

## Using the Application
Access the application through: `http://localhost:8000/`
![ui](/image/resource/chainlit-ui.png)

Features:
### 1. Chat Bot Interface
   - Interactive chatbot functionality
![ch1](/image/chainlit/1.png)
![ch2](/image/chainlit/2.png)

### 2. Pipeline Execution
   - Run scraping pipeline using the "Run Pipeline" button
     
![ch3](/image/chainlit/3.png)
![ch4](/image/chainlit/4.png)
### 3. Summary Generation
   - Generate summaries using the "Summarize Pipeline" button

![ch5](/image/chainlit/5.png)
![ch6](/image/chainlit/6.png)
![ch7](/image/chainlit/7.png)

### 4. Ask LLM for more detail
 ![ch8](/image/chainlit/8.png)

### 5. RAG fetches data from Database
   
![ch9](/image/chainlit/9.png)
![ch10](/image/chainlit/10.png)   

### 6. Chat History
   - Restore previous conversations

  ![ch11](/image/chainlit/11.png)
### 7. Persistent Storage
   - All data is persisted in the database
  ![data](/image/resource/database.png)
  ![datalog](/image/resource/conversation-log.png)
## Cleanup
To stop and clean up:
1. Stop containers:
```bash
Ctrl + C 
```
To stop running containers

![9](/image/reproduce/9.png)
2. Destroy infrastructure:
```bash
make infra-down
```
![10](/image/reproduce/10.png)
![11](/image/reproduce/11.png)
## Notes
- Ensure all environment variables are properly configured before running the application
- The service account key is required for proper functionality
- Data persistence is managed through the database setup
