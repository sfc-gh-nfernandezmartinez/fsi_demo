# FSI Demo - Snowflake Financial Services Platform

A comprehensive demonstration of Snowflake's capabilities for financial services, showcasing data ingestion, transformation, governance, and analytics.

## 🏦 Overview

This project demonstrates a modern data platform for a bank, featuring:
- Real-time transaction streaming
- Data governance with PII masking
- dbt transformations
- ML/AI analytics capabilities
- Streamlit dashboards

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Snowflake account with ACCOUNTADMIN access
- Snowflake CLI configured with key-pair authentication

### Installation
```bash
# Clone repository
git clone https://github.com/fernandezmartineznico/fsi_demo.git
cd fsi_demo

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Demo Usage
```bash
# Start transaction streaming
python stream_demo.py start

# Generate historical data
python stream_demo.py historical

# Clean up demo data
python stream_demo.py cleanup
```

## 📁 Project Structure
```
fsi_demo/
├── streaming/          # Transaction data generator
├── sql/               # Database setup scripts
├── dbt/               # Data transformation models
├── notebooks/         # Analytics and ML notebooks
├── stream_demo.py     # Main CLI tool
└── requirements.txt   # Python dependencies
```

## 👥 User Personas
- **data_engineer**: Data ingestion and pipeline management
- **data_analyst**: Business intelligence (with PII masking)
- **data_steward**: Data governance and policy management
- **data_scientist**: ML/AI and advanced analytics

## 🛡️ Security Features
- Key-pair authentication for services
- Role-based access control
- PII masking policies
- No credentials in code

## 📊 Data Model
- **CUSTOMER_TABLE**: Customer information (100 customers)
- **MORTGAGE_TABLE**: Loan details from AWS S3
- **TRANSACTIONS_TABLE**: Real-time transaction stream

## 🔧 Technology Stack
- **Platform**: Snowflake (native dbt, Streamlit, ML)
- **Language**: Python
- **Authentication**: Key-pair + SSO
- **Storage**: AWS S3 with Iceberg
- **CI/CD**: GitHub Actions + Snowflake CLI

## 📖 Documentation
Detailed architecture and design documentation available in the project (excluded from public repository for internal use).

## 🤝 Contributing
This is a demonstration project. For questions or suggestions, please open an issue.

## 📄 License
MIT License - see LICENSE file for details.
