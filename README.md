# 🏦 FSI Demo - Snowflake Financial Services Platform

A comprehensive demonstration of Snowflake's enterprise capabilities for financial services, showcasing modern data architecture, governance, and analytics.

## ✨ Key Features

**🔒 Data Governance & Security**
- Role-based PII masking (GDPR/CCPA compliant)
- Principle of least privilege access control
- Real-time governance enforcement

**📊 Advanced Analytics & ML**
- Statistical anomaly detection with Z-score analysis
- Snowflake ML integration for predictive analytics
- Interactive dashboards with business insights

**⚡ Real-time Data Processing** 
- Python & Java streaming capabilities
- CDC (Change Data Capture) patterns
- High-performance data ingestion

**🔄 Modern Data Transformation**
- dbt native in Snowflake Workspaces
- Automated data quality and testing
- Staging and mart layer separation

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Snowflake account with ACCOUNTADMIN access
- Snowflake CLI configured with key-pair authentication

### Installation & Setup
```bash
# Clone repository
git clone https://github.com/sfc-gh-nfernandezmartinez/fsi_demo.git
cd fsi_demo

# Install Python dependencies
pip install -r requirements.txt
```

### 🎯 Deployment Steps
**Foundation** → **Data Ingestion** → **Streaming** → **Complete Governance** → **Analytics**

```sql
-- Step 1: Foundation Setup (warehouses, roles, schemas)
@sql/01_foundation_setup.sql

-- Step 2: Data Ingestion & Schema (tables, customer/mortgage data)
@sql/02_ingestion_setup.sql

-- Step 3: CDC Streaming Setup (real-time transaction processing)
@sql/03_cdc_streaming_setup.sql

-- Step 4: Complete Governance Framework (PII masking, RBAC, compliance)
@sql/04_governance_complete.sql
```

**5. Deploy dbt Models (Snowflake native):**
```bash
# In Snowflake Workspaces, execute dbt project
execute dbt project from workspace USER$.PUBLIC.fsi_demo project_root='/dbt' args='run --target dev'
```

**6. Deploy Streamlit Dashboard:**
```bash
cd streamlit_app
snow streamlit deploy --replace
# Access at: FSI_DEMO.ANALYTICS.FSI_ANALYTICS_DASHBOARD
```

## 📁 Clean Project Structure
```
fsi_demo/
├── 📂 sql/                         # Complete SQL setup (4 scripts)
│   ├── 01_foundation_setup.sql     # Warehouses, roles, schemas
│   ├── 02_ingestion_setup.sql      # Tables, data loading
│   ├── 03_cdc_streaming_setup.sql  # Real-time streaming
│   └── 04_governance_complete.sql  # PII masking, RBAC, compliance
├── 📂 dbt/                         # Data transformation pipeline
│   ├── models/staging/             # Clean, standardized views
│   ├── models/marts/               # Business-ready tables
│   ├── dbt_project.yml            # dbt configuration
│   └── profiles.yml               # Snowflake connection
├── 📂 streaming/                   # Python data generators
│   ├── customer_generator.py      # Generate customer data
│   ├── transaction_generator.py   # Core transaction logic
│   ├── historical_generator.py    # Generate 200k historical records
│   └── simple_realtime_streamer.py # Real-time streaming
├── 📂 java_streaming/              # Enterprise CDC streaming
│   ├── src/snowflake/demo/samples/FSIEventStreamer.java
│   ├── README.md                  # Java setup instructions
│   └── snowflake.properties      # Configuration
├── 📂 streamlit_app/               # Analytics dashboard
│   ├── streamlit_app.py           # Main dashboard app
│   ├── snowflake.yml              # SnowCLI deployment config
│   └── environment.yml            # Python dependencies
├── 📂 Cursor_Designs/              # Architecture documentation
└── 📄 stream_demo.py               # CLI tool for data generation
```

## 🎯 Demo Usage

**Generate Data:**
```bash
# Generate customer data (5,000 customers)
python stream_demo.py customers

# Generate historical transactions (200,000 records)
python stream_demo.py historical

# Start real-time streaming
python stream_demo.py start
```

**Java CDC Streaming:**
```bash
cd java_streaming
# Build and run FSI event streamer
mvn compile exec:java -Dexec.mainClass="CDCSimulatorApp"
```

## 👥 User Personas & Roles

| Role | Access Level | Purpose |
|------|-------------|---------|
| **`data_steward`** | 🟢 Full PII Access | Governance oversight, compliance monitoring |
| **`data_analyst_role`** | 🔒 Masked PII | Dashboard users, business analytics |
| **`ACCOUNTADMIN`** | 🔒 Masked PII | System administration (enhanced security) |

## 📊 Data Model & Scale

**🏦 Customer Data** (5,000 customers)
- Customer profiles with PII (masked for compliance)
- 1:1 mapping to mortgage applications
- Realistic customer tier distribution

**🏠 Mortgage Data** (4,800 applications)
- Loan details from AWS S3 external stage
- Iceberg table format for performance
- Real loan application attributes

**💳 Transaction Data** (200,000+ historical + real-time)
- FSI transaction types (leisure, lifestyle, etc.)
- CDC streaming with INSERT/UPDATE/DELETE
- Statistical patterns for anomaly detection

## 🛠 Technology Stack

- **🏔️ Platform**: Snowflake Data Cloud
- **🔄 Transformations**: dbt Core (Snowflake-native execution)
- **📡 Real-time Streaming**: Python + Java (Snowpipe Streaming SDK)
- **📊 Analytics**: Streamlit + Plotly (interactive dashboards)
- **🤖 ML/AI**: Snowflake ML Functions (anomaly detection, forecasting)
- **🔒 Governance**: RBAC + Dynamic PII Masking Policies
- **☁️ Storage**: AWS S3 + Iceberg + Snowflake managed storage

## 🔍 Key Demonstrations

### 1. **Data Governance & Compliance**
- **Live PII Masking**: Different roles see different data
- **RBAC in Action**: Role-based access control
- **Compliance**: GDPR/CCPA ready governance framework

### 2. **Advanced Analytics & ML**
- **Real Anomaly Detection**: Statistical Z-score + Snowflake ML
- **Interactive Dashboards**: Multi-tab analytics with business insights
- **Pattern Analysis**: Weekly patterns, volume distribution, heatmaps

### 3. **Real-time Data Processing**
- **CDC Streaming**: Change data capture with actions
- **Java + Python**: Enterprise-grade streaming architecture
- **High Performance**: Optimized for financial transaction volumes

### 4. **Modern Data Pipeline**
- **dbt Native**: Transformations running in Snowflake Workspaces
- **Staging → Marts**: Clean data architecture
- **Data Quality**: Built-in testing and validation

## 🏆 Business Value Delivered

✅ **Reduced Compliance Risk** - Automatic PII masking and governance  
✅ **Real-time Insights** - Live transaction monitoring and anomaly detection  
✅ **Operational Efficiency** - Automated data pipelines and transformations  
✅ **Enhanced Security** - Role-based access and principle of least privilege  
✅ **Scalable Architecture** - Cloud-native, elastic compute and storage  

## 📖 Documentation

Detailed architecture and design documentation available in `Cursor_Designs/` (internal use).

**Key Documents:**
- `01_Project_Architecture.md` - Overall system design
- `02_Data_Pipeline.md` - Streaming and governance
- `03_Analytics_Transformation.md` - dbt and ML strategy

## 🤝 Contributing

This is a demonstration project showcasing Snowflake capabilities for financial services. For questions or suggestions, please open an issue.

## 📄 License

MIT License - see LICENSE file for details.

---

**🏦 Built by Snowflake for Financial Services Innovation**  
*Demonstrates: PII Masking • Real-time Analytics • ML Integration • Enterprise Governance*