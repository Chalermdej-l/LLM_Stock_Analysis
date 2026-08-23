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
   - [5. SQL Tool Queries](#5-sql-tool-queries)
   - [6. Chat History](#6-chat-history)
   - [7. Persistent Storage](#7-persistent-storage)
6. [Cleanup](#cleanup)

## Prerequisites

- Git
- Terraform
- GNU Make (install from your platform's package manager, not PyPI)
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

2. Configure environment variables by copying the [.env](../.env.example) template:
    - Rename `.env.example` to `.env`
    - Update the necessary variables in the `.env` file (Groq `API_KEY`, Google `PROJECT_ID`, Magic Formula credentials, SQL credentials, and the `MODEL` / `MODEL_TOOL` model ids)

Also update the Google Project ID in the Terraform [variables](infra/terraform.tfvars) configuration.

## Infrastructure Setup

1. Initialize and plan the infrastructure:
```bash
make infra-init
```

After you review the resources to create, run it to create the infra:
```bash
make infra-up
```

> Note: Infrastructure creation may take approximately 10 minutes to complete

This will create the IAM account

![iam](/image/resource/iam.png)

the service account

![sa](/image/resource/sa.png)

and the Cloud SQL database

![sql](/image/resource/sql.png)

2. Run the command below to output the service account key. This creates a new folder called `key` and puts the service account key inside:
```bash
terraform -chdir=infra output -raw service_account_key | python stock_analysis/decode_key.py --encode_key="$(cat)"
```

> Note: this is a long-lived, downloadable key. For a production deployment, prefer Workload Identity Federation over a service-account key.

## Docker Setup

1. Prepare the Docker environment:
    - Ensure the `.env` file is properly configured

2. Build the Docker image:
```bash
make docker-build
```

Then run the containers:
```bash
make docker-up
```

> Note: the init-db service is only used to create the tables in the database on the first run of the stack.

![docker](/image/resource/docker.png)

This will spawn three containers: the Chainlit app, the one-shot init-db job, and the Cloud SQL proxy.

![app](/image/resource/chainlit.png)
![proxy](/image/resource/proxy.png)

## Using the Application

Access the application through: `http://localhost:8000/`

![ui](/image/resource/chainlit-ui.png)

Features:

### 1. Chat Bot Interface
- Interactive chatbot functionality

### 2. Pipeline Execution
- Run the scraping pipeline using the "Run Pipeline" button

### 3. Summary Generation
- Generate summaries using the "Summarize Pipeline" button

### 4. Ask LLM for more detail
- Ask follow-up questions about any generated report

### 5. SQL Tool Queries
- For database questions, the LLM writes a PostgreSQL query, executes it against a read-only database user, and summarizes the result

### 6. Chat History
- Restore previous conversations

### 7. Persistent Storage
- All data is persisted in the database

![data](/image/resource/database.png)
![datalog](/image/resource/conversation-log.png)

## Cleanup

To stop and clean up:

1. Stop the containers:
```bash
Ctrl + C
```
to stop the running containers.

2. Destroy the infrastructure:
```bash
make infra-down
```

## Notes

- Ensure all environment variables are properly configured before running the application
- The service account key in `key/` is required for the Cloud SQL proxy
- Data persistence is managed through the database setup
