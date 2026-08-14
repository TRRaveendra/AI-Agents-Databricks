## 🛡️   Insurance — 20 AI Agents

Insurance is an excellent next domain because it combines **claims, underwriting, policy documents, fraud detection, risk analytics, customer 360, pricing, regulatory compliance, and real-time event processing**.

|  # | AI Agent                                     | What the Agent Does                                                                                | Key Databricks Capability |
| -: | -------------------------------------------- | -------------------------------------------------------------------------------------------------- | ------------------------- |
|  1 | 🧾 **Claims Processing Agent**               | Reviews claims, extracts information, validates coverage and identifies missing evidence           | Document AI + RAG         |
|  2 | 🔍 **Claims Investigation Agent**            | Investigates suspicious or complex claims using policy, customer, incident and historical data     | SQL + RAG + Tool Calling  |
|  3 | 🛡️ **Insurance Fraud Detection Agent**      | Detects suspicious claim, customer, provider and transaction patterns                              | Streaming + ML + AI       |
|  4 | 📋 **Underwriting Agent**                    | Analyzes applications, risk information and underwriting guidelines to prepare recommendations     | RAG + Document AI         |
|  5 | 📜 **Policy Intelligence Agent**             | Answers questions about coverage, exclusions, deductibles, limits and policy conditions            | RAG + AI Search           |
|  6 | 👤 **Insurance Customer 360 Agent**          | Combines policies, claims, payments, interactions and customer history into a unified view         | Customer 360 + SQL        |
|  7 | ⚠️ **Risk Assessment Agent**                 | Evaluates customer/property/vehicle/business risk using historical and external data               | ML + Analytics            |
|  8 | 💰 **Insurance Pricing Agent**               | Analyzes risk factors, historical claims and market information to support pricing decisions       | ML + Optimization         |
|  9 | 🚗 **Auto Insurance Intelligence Agent**     | Analyzes vehicle telemetry, accidents, claims and driving behavior                                 | Streaming + IoT           |
| 10 | 🏠 **Property Risk Agent**                   | Assesses property risks using location, historical claims, weather and property characteristics    | Geospatial + RAG          |
| 11 | 🌪️ **Catastrophe Risk Agent**               | Monitors weather and catastrophe events and identifies potentially affected insured assets         | Streaming + Geospatial    |
| 12 | 📈 **Claims Forecasting Agent**              | Forecasts future claim volumes, severity and operational workload                                  | Time Series + ML          |
| 13 | 🧮 **Loss Ratio Intelligence Agent**         | Analyzes premiums, claims and exposure to explain changes in loss ratios                           | SQL + Financial Analytics |
| 14 | 🏥 **Healthcare Insurance Agent**            | Analyzes medical claims, eligibility, benefits and authorization information                       | RAG + Document AI         |
| 15 | 🏢 **Commercial Insurance Agent**            | Assesses business insurance risks across property, liability, operations and financial information | RAG + Analytics           |
| 16 | 💳 **Premium & Billing Agent**               | Investigates premium calculations, payments, missed payments and billing discrepancies             | SQL + Tool Calling        |
| 17 | 📞 **Insurance Customer Service Agent**      | Answers policy, claims, billing and coverage questions using customer-specific information         | RAG + Customer 360        |
| 18 | 📜 **Insurance Compliance Agent**            | Reviews policies, claims and processes against regulations and internal controls                   | RAG + AI Search           |
| 19 | 🔄 **Renewal & Retention Agent**             | Identifies policies at renewal risk and recommends retention strategies                            | ML + Customer 360         |
| 20 | 🤖 **Insurance Operations Supervisor Agent** | Coordinates claims, underwriting, fraud, pricing, risk, compliance and customer agents             | Multi-Agent + Supervisor  |

---

# ⭐ Top Student Projects

### 1. 🧾 Claims Processing Agent

```text
Customer Claim
      ↓
Documents / Images / Forms
      ↓
Document Intelligence
      ↓
Policy Retrieval
      ↓
Claims Processing Agent
      ↓
Coverage Validation
      ↓
Missing Information
      ↓
Claims Recommendation
      ↓
Human Review
```

Example:

> **"Does this claim appear to be covered by the customer's policy, and what evidence is still missing?"**

---

### 2. 🛡️ Insurance Fraud Detection Agent

```text
Claims
  +
Customers
  +
Policies
  +
Providers
  +
Payments
  +
Historical Claims
       ↓
Fraud Detection
       ↓
Anomaly Detection
       ↓
Fraud Agent
       ↓
Risk Score
       ↓
Investigation
```

Advanced students can turn this into a **graph investigation agent** connecting:

```text
Customer → Policy → Claim → Provider → Vehicle/Property → Payment
```

---

### 3. 📋 Underwriting Agent

```text
Application
     +
Financial Documents
     +
Risk Information
     +
Historical Claims
     +
Underwriting Guidelines
        ↓
Underwriting Agent
        ↓
Risk Assessment
        ↓
Evidence
        ↓
Underwriting Recommendation
```

---

### 4. 📜 Policy Intelligence Agent

```text
Insurance Policies
       ↓
Document Processing
       ↓
Chunking + Embeddings
       ↓
AI Search
       ↓
Policy Intelligence Agent
       ↓
Coverage / Exclusions / Limits
       ↓
Answer
```

Example:

> **"What exclusions apply to this type of claim?"**

---

### 5. 🌪️ Catastrophe Risk Agent

```text
Weather Data
     +
Satellite / Geospatial Data
     +
Policy Locations
     +
Historical Claims
       ↓
Catastrophe Agent
       ↓
Affected Assets
       ↓
Risk Assessment
       ↓
Claims Forecast
       ↓
Operations Alert
```

This is an excellent **real-time + geospatial + AI Agent** project.

---

# 🤖 Ultimate Capstone: Insurance Operations Supervisor

```text
                    ┌─────────────────────────┐
                    │ Insurance Operations    │
                    │    Supervisor Agent     │
                    └────────────┬────────────┘
                                 │
       ┌───────────┬─────────────┼─────────────┬─────────────┐
       ↓           ↓             ↓             ↓             ↓
 Claims Agent  Underwriting   Fraud Agent   Risk Agent   Policy Agent
                 Agent
       ↓           ↓             ↓             ↓             ↓
 Investigation   Risk         Detection    Assessment    Coverage
       │           │             │             │             │
       └───────────┴─────────────┼─────────────┴─────────────┘
                                 ↓
                       Compliance Agent
                                 ↓
                       Customer Agent
                                 ↓
                         Decision Support
                                 ↓
                         Human Approval
```

## 🎓 Learning Progression

| Level           | Projects                                                              | Skills                          |
| --------------- | --------------------------------------------------------------------- | ------------------------------- |
| 🟢 Beginner     | Policy, Customer Service, Claims, Compliance                          | RAG + SQL                       |
| 🟡 Intermediate | Billing, Customer 360, Underwriting, Renewal                          | RAG + Document AI               |
| 🟠 Advanced     | Fraud, Risk, Pricing, Claims Forecasting                              | ML + Streaming                  |
| 🔴 Expert       | Catastrophe, Property Risk, Fraud Investigation, Healthcare Insurance | Geospatial + Graph + Agents     |
| 🏆 Capstone     | **Insurance Operations Supervisor**                                   | Multi-Agent + Human-in-the-Loop |

### Ideal sequence

**Policy Intelligence → Claims Agent → RAG Agent → Customer 360 → Document AI → Underwriting → Fraud Detection → Risk Agent → Real-Time Catastrophe Agent → Multi-Agent Insurance Supervisor**
