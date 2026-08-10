# 🏦 20 AI Agents for Banking

Banking is one of the best domains for your students because it covers **real-time transactions, fraud, KYC/AML, credit risk, loans, customer 360, financial documents, compliance, personalization, and multi-agent systems**.

|  # | AI Agent                                   | What the Agent Does                                                                                      | Key Databricks Capability     |
| -: | ------------------------------------------ | -------------------------------------------------------------------------------------------------------- | ----------------------------- |
|  1 | 🧑‍💼 **Banking Customer Support Agent**   | Answers customer questions about accounts, cards, transactions, products and banking services            | RAG + AI Search               |
|  2 | 👤 **Customer 360 Intelligence Agent**     | Builds a unified view of customer accounts, transactions, products, interactions and behavior            | Customer 360 + SQL Agent      |
|  3 | 🛡️ **Banking Fraud Detection Agent**      | Detects suspicious transactions and unusual customer behavior in real time                               | Streaming + ML + AI Agent     |
|  4 | 🔍 **Fraud Investigation Agent**           | Investigates suspicious transactions by correlating accounts, devices, locations and transaction history | SQL + RAG + Tool Calling      |
|  5 | 🪪 **KYC Verification Agent**              | Validates identity documents, customer information and KYC requirements                                  | Document AI + RAG             |
|  6 | 🚨 **AML Investigation Agent**             | Identifies suspicious transaction patterns and assists AML investigators with case analysis              | Streaming + Graph + RAG       |
|  7 | 💳 **Credit Risk Assessment Agent**        | Analyzes customer financial behavior and credit information to assess risk                               | ML + SQL + AI                 |
|  8 | 🏦 **Loan Underwriting Agent**             | Reviews loan applications, financial documents, policies and eligibility criteria                        | RAG + Document AI             |
|  9 | 📄 **Loan Document Intelligence Agent**    | Extracts and validates information from income statements, bank statements and supporting documents      | Document AI + RAG             |
| 10 | 💰 **Loan Portfolio Monitoring Agent**     | Monitors loan portfolios for delinquency, risk concentration and changing customer behavior              | SQL + Analytics               |
| 11 | 📉 **Loan Default Prediction Agent**       | Identifies customers or loans showing increased probability of default and explains risk factors         | ML + Time Series              |
| 12 | 💸 **Collections Intelligence Agent**      | Prioritizes delinquent accounts and recommends appropriate collection strategies                         | Customer Analytics + AI       |
| 13 | 💳 **Credit Card Intelligence Agent**      | Analyzes card spending, utilization, transactions, rewards and customer behavior                         | Streaming + Customer 360      |
| 14 | 📈 **Next-Best-Product Agent**             | Recommends suitable banking products such as cards, loans, deposits or investments                       | Recommendation + Customer 360 |
| 15 | 🏦 **Branch Operations Agent**             | Analyzes branch performance, customer traffic, staffing, transactions and operational issues             | SQL + Analytics               |
| 16 | 📊 **Banking Analytics Agent**             | Answers business questions about deposits, loans, transactions, revenue, customers and branches          | Text-to-SQL + BI              |
| 17 | 📜 **Banking Compliance Agent**            | Reviews policies, regulations and operational evidence to identify compliance gaps                       | RAG + AI Search               |
| 18 | 🔄 **Payment & Reconciliation Agent**      | Reconciles transactions between banking systems, payment networks and ledgers                            | SQL + Lakehouse               |
| 19 | 📢 **Customer Churn Prevention Agent**     | Identifies customers likely to leave and recommends personalized retention actions                       | ML + Customer 360             |
| 20 | 🤖 **Banking Operations Supervisor Agent** | Coordinates fraud, KYC, AML, loans, risk, compliance, customer and reconciliation agents                 | Multi-Agent + Supervisor      |

---

# ⭐ Top Student Projects

## 1. 🛡️ Banking Fraud Detection Agent

One of the strongest **real-time Agentic AI** projects.

```text id="bank01"
Bank Transactions
      ↓
Kafka / Event Hub
      ↓
Databricks Structured Streaming
      ↓
Delta Lake
      ↓
Fraud Detection Model
      ↓
Fraud Agent
      ↓
Risk Score
      ↓
 ┌─────────┬─────────┬─────────┐
 ↓         ↓         ↓
Approve   Review    Block*
```

*In a student/production design, consequential actions should use appropriate authorization and human-review controls.

Example:

> **"Why was this transaction classified as high risk?"**

The agent can correlate transaction history, device, location, merchant and behavioral signals and produce an explanation.

---

# 2. 👤 Customer 360 Intelligence Agent

```text id="bank02"
Customer
   ↓
Accounts
   +
Transactions
   +
Loans
   +
Credit Cards
   +
Investments
   +
Support History
   +
Digital Activity
      ↓
Customer 360 Agent
      ↓
Unified Customer Profile
      ↓
Insights
```

Example:

> **"Summarize this customer's banking relationship and identify their major financial products."**

This is excellent for teaching **Lakehouse + SQL Agent + Customer 360**.

---

# 3. 🪪 KYC Verification Agent

```text id="bank03"
Customer Application
        ↓
Identity Documents
        ↓
Document Processing
        ↓
Information Extraction
        ↓
Customer Records
        ↓
KYC Agent
        ↓
 ┌──────────┬───────────┐
 ↓          ↓           ↓
Verified   Review     Exception
```

The agent can explain which information is missing or inconsistent.

---

# 4. 🚨 AML Investigation Agent

```text id="bank04"
Transactions
     +
Accounts
     +
Customers
     +
Beneficiaries
     +
Locations
     ↓
AML Agent
     ↓
Pattern Detection
     ↓
Relationship Analysis
     ↓
Investigation
     ↓
Case Summary
```

For an advanced student project, represent relationships as a **graph**:

```text id="bank05"
Customer
   ↓
Account
   ↓
Transaction
   ↓
Beneficiary
   ↓
Another Account
   ↓
Another Customer
```

This allows students to explore **graph analytics + agentic investigation**.

---

# 5. 🏦 Loan Underwriting Agent

```text id="bank06"
Loan Application
      ↓
Customer Profile
      +
Income Documents
      +
Credit History
      +
Bank Statements
      +
Loan Policy
      ↓
Underwriting Agent
      ↓
Eligibility Analysis
      ↓
Risk Analysis
      ↓
Underwriting Summary
      ↓
Human Review
```

This is a very strong **Document AI + RAG + SQL + Agent** project.

---

# 6. 💳 Credit Risk Assessment Agent

```text id="bank07"
Customer
   +
Credit History
   +
Income
   +
Existing Loans
   +
Repayment Behavior
   +
Transactions
      ↓
Credit Risk Agent
      ↓
Risk Assessment
      ↓
Risk Drivers
      ↓
Credit Analyst Recommendation
```

---

# 7. 📉 Loan Default Prediction Agent

```text id="bank08"
Loan Portfolio
      ↓
Repayment History
      ↓
Customer Behavior
      ↓
Financial Signals
      ↓
Default Prediction
      ↓
AI Agent
      ↓
Risk Explanation
      ↓
Early Intervention
```

The important learning point is combining **predictive ML with an agent that explains and investigates the prediction**.

---

# 8. 💸 Collections Intelligence Agent

```text id="bank09"
Delinquent Loans
      ↓
Customer History
      +
Payment Behavior
      +
Risk Profile
      +
Previous Contacts
      ↓
Collections Agent
      ↓
Customer Prioritization
      ↓
Recommended Strategy
      ↓
Collections Workflow
```

---

# 9. 💳 Credit Card Intelligence Agent

```text id="bank10"
Card Transactions
      +
Merchant Data
      +
Spending Patterns
      +
Rewards
      +
Credit Utilization
      ↓
Card Intelligence Agent
      ↓
Spending Insights
      ↓
Risk Detection
      ↓
Personalized Recommendations
```

---

# 10. 📈 Next-Best-Product Agent

```text id="bank11"
Customer 360
      +
Financial Behavior
      +
Existing Products
      +
Eligibility
      +
Product Catalog
      ↓
Next-Best-Product Agent
      ↓
 ┌────────┬────────┬─────────┐
 ↓        ↓        ↓
Credit   Loan     Deposit
Card              Product
      ↓
Recommendation
```

Example:

> **"Which banking product is most relevant to this customer, and why?"**

---

# 11. 📊 Banking Analytics Agent

A powerful **Text-to-SQL** project.

```text id="bank12"
Business Question
       ↓
Banking Analytics Agent
       ↓
Generate SQL
       ↓
Databricks SQL
       ↓
Banking Lakehouse
       ↓
Analysis
       ↓
Business Explanation
```

Students can ask:

> "Which branch had the highest loan growth this quarter?"

> "What is the NPA trend by region?"

> "Which customer segment has the highest deposit growth?"

---

# 12. 🔄 Payment & Reconciliation Agent

```text id="bank13"
Core Banking
      +
Payment Gateway
      +
Bank Ledger
      +
Settlement System
      ↓
Databricks Lakehouse
      ↓
Reconciliation Agent
      ↓
Matched Transactions
      +
Exceptions
      ↓
Root Cause
      ↓
Exception Report
```

This is particularly good for your **Data Engineering + Agentic AI** training because students can see how traditional ETL/data-quality workflows become agentic.

---

# 13. 📜 Banking Compliance Agent

```text id="bank14"
Regulations
     +
Bank Policies
     +
Procedures
     +
Audit Evidence
     +
Operational Data
      ↓
Compliance Agent
      ↓
Policy Retrieval
      ↓
Control Mapping
      ↓
Gap Analysis
      ↓
Compliance Report
```

---

# 14. 📢 Customer Churn Prevention Agent

```text id="bank15"
Customer Behavior
      +
Transaction Activity
      +
Product Usage
      +
Support Interactions
      +
Digital Activity
      ↓
Churn Prediction
      ↓
Churn Prevention Agent
      ↓
Root Cause
      ↓
Retention Recommendation
```

---

# 🤖 Ultimate Capstone: Banking Operations Supervisor

The 20 individual agents can eventually become a **multi-agent banking platform**:

```text id="bank16"
                         ┌──────────────────────────┐
                         │ Banking Operations       │
                         │      Supervisor Agent    │
                         └────────────┬─────────────┘
                                      │
       ┌──────────────┬───────────────┼──────────────┬──────────────┐
       ↓              ↓               ↓              ↓              ↓
 Fraud Agent      KYC Agent       AML Agent      Credit Agent    Loan Agent
       ↓              ↓               ↓              ↓              ↓
Investigation    Verification   Investigation    Risk Score     Underwriting
       │              │               │              │              │
       └──────────────┴───────────────┼──────────────┴──────────────┘
                                      ↓
                             Compliance Agent
                                      ↓
                           Reconciliation Agent
                                      ↓
                             Decision Support
                                      ↓
                              Human Approval
```

# 🏦 Databricks Banking Architecture

```text id="bank17"
                    BANKING DATA
                         │
      ┌──────────────────┼──────────────────┐
      ↓                  ↓                  ↓
 Core Banking       Digital Banking      External
      ↓                  ↓                  ↓
 Accounts           Mobile/Web          Credit Data
 Transactions       Clickstream         Market Data
 Loans              Login Events        Regulations
 Cards              Customer Events     Documents
      │                  │                  │
      └──────────────────┼──────────────────┘
                         ↓
              Kafka / APIs / Files
                         ↓
          Databricks Structured Streaming
                         ↓
                    Delta Lake
                         ↓
                 Unity Catalog
                         ↓
                  Customer 360
                         ↓
                  AI Agent Layer
                         ↓
       ┌─────────────────┼─────────────────┐
       ↓                 ↓                 ↓
    RAG Agent         SQL Agent        Tool Agent
       ↓                 ↓                 ↓
       └─────────────────┼─────────────────┘
                         ↓
                  Multi-Agent Layer
                         ↓
              Banking Supervisor
                         ↓
                 Human-in-the-Loop
                         ↓
                  Databricks App
```

# 🎓 Recommended   Progression

| Level               | Projects                                                                        | Skills                          |
| ------------------- | ------------------------------------------------------------------------------- | ------------------------------- |
| 🟢 **Beginner**     | Banking Support, Customer 360, Banking Analytics, Compliance, Next-Best-Product | RAG + SQL                       |
| 🟡 **Intermediate** | KYC, Loan Documents, Reconciliation, Credit Risk, Customer Churn                | Document AI + Tool Calling      |
| 🟠 **Advanced**     | Fraud, AML, Loan Underwriting, Default Prediction, Credit Card Intelligence     | Streaming + ML + Agents         |
| 🔴 **Expert**       | Fraud Investigation, AML Graph, Risk, Collections, Compliance                   | Graph + Multi-Tool Agents       |
| 🏆 **Capstone**     | **Banking Operations Supervisor**                                               | Multi-Agent + Human-in-the-Loop |

### Ideal learning sequence

**Banking Support Agent → RAG Agent → SQL Banking Agent → Customer 360 → KYC Agent → Loan Agent → Fraud Agent → AML Agent → Real-Time Banking Agent → Multi-Agent Banking Supervisor**

This gives a very strong enterprise progression:

**Databricks Lakehouse → Delta Lake → Structured Streaming → SQL → AI Search/RAG → Document Intelligence → Customer 360 → ML → Tool Calling → Graph Analytics → Multi-Agent Systems → Human-in-the-Loop.**
