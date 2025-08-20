# 🏦 FSI Demo - Snowflake Financial Services Platform

A concise demonstration of Snowflake’s data platform for financial services: ingestion, transformation, governance, visualization, and analytics.

## 🧭 Architecture & Lineage

```mermaid
flowchart LR
  subgraph Sources ["Data Sources"]
    s3["Historical CSV Files<br/>(AWS S3)"]
    py["Real-time Payments Stream<br/>(Python Generators)"]
    java["Database CDC Events<br/>(Java Simulator)"]
  end

  subgraph Snowflake ["Snowflake Data Cloud"]
    raw["Raw Data Layer"]
    dbt["dbt Transformations"]
    
    subgraph layers [" "]
      dynamic["Dynamic Tables"]
      governance["Data Governance"]
    end
    
    marts["Analytics Marts"]
    streamlit["Streamlit Dashboard"]
  end

  s3 --> raw
  py --> raw
  java --> raw
  raw --> dbt
  dbt --> dynamic
  dbt --> governance
  dynamic --> marts
  governance -.-> marts
  marts --> streamlit
```

## ✨ Key Features (In Demo Order)

- **Ingestion**: Batch from S3, Python generators, and Java CDC streaming
- **Transformation**: dbt staging (views in `TRANSFORMED`) and marts (tables in `ANALYTICS`)
- **Governance**: Strict PII masking policies (only `data_steward` unmasked), least-privilege roles
- **Visualization**: Native Streamlit app in `ANALYTICS` schema
- **Analytics**: Customer 360, transaction summaries, simple insights

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

### 🎯 Deployment Steps (use in your live demo)
1) Foundation
```sql
@sql/01_foundation_setup.sql
```

2) Ingestion (tables and data)
```sql
@sql/02_ingestion_setup.sql
```

3) CDC Streaming Setup (optional for demo)
```sql
@sql/03_cdc_streaming_setup.sql
```

4) Governance (strict masking policies and RBAC)
```sql
@sql/04_governance.sql
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
├── sql/                           # Idempotent setup scripts
│   ├── 01_foundation_setup.sql    # Warehouses, roles, schemas
│   ├── 02_ingestion_setup.sql     # Tables, data loading
│   ├── 03_cdc_streaming_setup.sql # CDC streaming demo
│   └── 04_governance.sql          # PII masking, RBAC
├── dbt/                           # Data transformation pipeline
│   ├── models/staging/            # Clean, standardized views
│   ├── models/marts/              # Business-ready tables
│   ├── dbt_project.yml            # dbt configuration
│   └── profiles.yml               # Snowflake connection
├── streaming/                     # Python data generators
│   ├── customer_generator.py
│   ├── transaction_generator.py
│   ├── historical_generator.py
│   └── simple_realtime_streamer.py
├── java_streaming/                # Java CDC streaming
│   ├── CDCSimulatorClient.jar
│   ├── src/snowflake/demo/samples/FSIEventStreamer.java
│   └── snowflake.properties
├── streamlit_app/                 # Native Streamlit app
│   ├── streamlit_app.py
│   ├── snowflake.yml
│   └── environment.yml
├── guides/                        # Demo documentation
│   ├── 01_Project_Architecture.md
│   ├── 02_Data_Pipeline.md
│   └── 03_Analytics_Transformation.md
└── stream_demo.py                 # CLI for data generation
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

## 🔍 What to Show in the Demo

1) Ingestion: S3 copy, generated customers, optional CDC
2) Transformation: dbt staging and marts in the right schemas
3) Governance: Query `CUSTOMER_TABLE` as `data_analyst_role` vs `data_steward`
4) Visualization: Open the Streamlit app in Snowflake and apply filters
5) Analytics: Query `ANALYTICS.customer_360` and `ANALYTICS.transaction_summary`

## 🏆 Business Value Delivered

✅ **Reduced Compliance Risk** - Automatic PII masking and governance  
✅ **Real-time Insights** - Live transaction monitoring and anomaly detection  
✅ **Operational Efficiency** - Automated data pipelines and transformations  
✅ **Enhanced Security** - Role-based access and principle of least privilege  
✅ **Scalable Architecture** - Cloud-native, elastic compute and storage  

## 📖 Documentation

See `guides/` for the demo runbook:
- `01_Project_Architecture.md` — overall design and lineage
- `02_Data_Pipeline.md` — ingestion, streaming, governance, optional DMFs & audit
- `03_Analytics_Transformation.md` — dbt staging/marts and validation

## 🤝 Contributing

This is a demonstration project showcasing Snowflake capabilities for financial services. For questions or suggestions, please open an issue.

## 📄 License

MIT License - see LICENSE file for details.

---

**🏦 Built by Snowflake for Financial Services Innovation**  
*Demonstrates: PII Masking • Real-time Analytics • ML Integration • Enterprise Governance*