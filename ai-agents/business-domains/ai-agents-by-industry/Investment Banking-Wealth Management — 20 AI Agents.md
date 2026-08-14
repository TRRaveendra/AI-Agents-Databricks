# 🏦   Investment Banking / Wealth Management — 20 AI Agents

This is a strong **Databricks Agentic AI** domain because it combines **client 360, portfolio analytics, market intelligence, research, risk, financial documents, transactions, compliance, wealth planning, and real-time market data**.

|  # | AI Agent                                      | What the Agent Does                                                                            | Key Databricks Capability     |
| -: | --------------------------------------------- | ---------------------------------------------------------------------------------------------- | ----------------------------- |
|  1 | 📊 **Investment Research Agent**              | Researches companies, industries, financials, filings and market information                   | RAG + AI Search               |
|  2 | 🏢 **Company Intelligence Agent**             | Builds a 360° view of a company using financial, market, news and business data                | RAG + SQL                     |
|  3 | 📈 **Equity Analysis Agent**                  | Analyzes revenue, earnings, valuation metrics and financial performance                        | SQL + Financial Analytics     |
|  4 | 💼 **Portfolio Intelligence Agent**           | Analyzes portfolio holdings, allocation, performance and concentration                         | SQL + Analytics               |
|  5 | ⚖️ **Portfolio Risk Agent**                   | Identifies concentration, volatility, exposure and other portfolio risks                       | ML + Risk Analytics           |
|  6 | 🎯 **Asset Allocation Agent**                 | Supports allocation analysis across equities, bonds, alternatives and other asset classes      | Optimization + Analytics      |
|  7 | 🔮 **Market Forecasting Agent**               | Analyzes market trends and generates scenario-based forecasts                                  | Time Series + ML              |
|  8 | 📰 **Market News Intelligence Agent**         | Processes market news and identifies potentially relevant events and themes                    | Streaming + NLP + RAG         |
|  9 | 🚨 **Market Event Detection Agent**           | Detects significant market movements and correlates them with events                           | Streaming + Anomaly Detection |
| 10 | 🧮 **Financial Modeling Agent**               | Assists with financial models, assumptions, scenarios and sensitivity analysis                 | SQL + Python Tools            |
| 11 | 📑 **Financial Document Agent**               | Extracts information from annual reports, filings, presentations and financial documents       | Document AI + RAG             |
| 12 | 🏦 **M&A Research Agent**                     | Supports target research, transaction analysis and comparable-company research                 | RAG + Financial Analytics     |
| 13 | 🤝 **Due Diligence Agent**                    | Analyzes financial, operational and contractual information during due diligence               | RAG + Document AI             |
| 14 | 💰 **Wealth Advisory Agent**                  | Provides portfolio and financial insights based on approved client objectives and constraints  | Customer 360 + Analytics      |
| 15 | 👤 **Wealth Client 360 Agent**                | Unifies holdings, transactions, goals, risk profile and client interactions                    | Customer 360                  |
| 16 | 📜 **Regulatory Compliance Agent**            | Searches regulations, policies and compliance requirements and supports control analysis       | RAG + AI Search               |
| 17 | 🛡️ **AML Investigation Agent**               | Investigates suspicious transaction patterns and creates evidence-based case summaries         | Graph + Streaming + RAG       |
| 18 | 💳 **Transaction Intelligence Agent**         | Analyzes investment transactions, settlements and reconciliation exceptions                    | Streaming + SQL               |
| 19 | 📊 **Performance Attribution Agent**          | Explains portfolio performance by asset, sector, geography and strategy                        | SQL + Financial Analytics     |
| 20 | 🤖 **Investment Operations Supervisor Agent** | Coordinates research, portfolio, risk, compliance, transactions and client-intelligence agents | Multi-Agent + Supervisor      |

> For students, these should be framed as **research, analytics and decision-support systems**. They should not autonomously execute trades or make regulated financial recommendations.

---

# ⭐ Top Student Projects

## 1. 📊 Investment Research Agent

```text id="ib01"
Company
   ↓
Financial Statements
   +
SEC / Regulatory Filings
   +
News
   +
Industry Reports
   +
Market Data
      ↓
Investment Research Agent
      ↓
Evidence Retrieval
      ↓
Financial Analysis
      ↓
Research Summary
```

Example:

> **"Summarize the company's latest financial performance and identify the major business drivers."**

---

# 2. 🏢 Company Intelligence Agent

```text id="ib02"
Company
  ↓
 ┌──────────┬──────────┬──────────┬──────────┐
 ↓          ↓          ↓          ↓
Financials  News      Products   Industry
 ↓          ↓          ↓          ↓
Competitors  Management  Market
       ↓
Company 360
       ↓
Company Intelligence Agent
```

---

# 3. 📈 Equity Analysis Agent

```text id="ib03"
Financial Statements
      +
Revenue
      +
Earnings
      +
Margins
      +
Valuation
      +
Peer Companies
      ↓
Equity Analysis Agent
      ↓
Financial Metrics
      ↓
Peer Comparison
      ↓
Investment Research
```

---

# 4. 💼 Portfolio Intelligence Agent

```text id="ib04"
Portfolio
   ↓
Holdings
 +
Transactions
 +
Asset Allocation
 +
Performance
 +
Exposure
      ↓
Portfolio Agent
      ↓
Portfolio 360
      ↓
Performance
      ↓
Risk Insights
```

Example:

> **"Show the portfolio's sector and geographic concentration."**

---

# 5. ⚖️ Portfolio Risk Agent

```text id="ib05"
Portfolio
   +
Market Data
   +
Historical Returns
   +
Volatility
   +
Correlation
      ↓
Risk Agent
      ↓
Risk Metrics
      ↓
Concentration
      ↓
Scenario Analysis
      ↓
Risk Report
```

---

# 6. 🎯 Asset Allocation Agent

```text id="ib06"
Client Objectives
      +
Portfolio
      +
Risk Constraints
      +
Asset Classes
      +
Market Scenarios
      ↓
Allocation Agent
      ↓
Optimization
      ↓
Allocation Scenarios
      ↓
Human / Advisor Review
```

---

# 7. 🔮 Market Forecasting Agent

```text id="ib07"
Market Data
   +
Historical Prices
   +
Economic Indicators
   +
Volumes
   +
Market Events
      ↓
Forecasting Model
      ↓
Market Forecast Agent
      ↓
Scenario Analysis
      ↓
Risk / Opportunity Insights
```

Use **scenario analysis**, rather than presenting forecasts as certain outcomes.

---

# 8. 📰 Market News Intelligence Agent

```text id="ib08"
News
  +
Financial Articles
  +
Company Announcements
  +
Regulatory Events
      ↓
Streaming
      ↓
News Intelligence Agent
      ↓
Entity Extraction
      ↓
Sentiment / Topic
      ↓
Market Relevance
```

---

# 9. 🚨 Market Event Detection Agent

Excellent **real-time Databricks** project.

```text id="ib09"
Market Prices
     +
Volumes
     +
News
     +
Corporate Events
      ↓
Kafka / Streaming
      ↓
Event Detection Agent
      ↓
Anomaly Detection
      ↓
Significant Event
      ↓
Research Alert
```

Example:

> **"Which stocks experienced unusual price and volume activity today?"**

---

# 10. 🧮 Financial Modeling Agent

```text id="ib10"
Business Assumptions
       ↓
Financial Modeling Agent
       ↓
Revenue Model
       +
Cost Model
       +
Cash Flow
       +
Valuation
       ↓
Scenario Analysis
       ↓
Sensitivity Analysis
```

Students can build tool functions for calculations instead of asking the LLM to perform all numerical work itself.

---

# 11. 📑 Financial Document Agent

Excellent **Document AI + RAG** project.

```text id="ib11"
Annual Reports
     +
10-K / 10-Q
     +
Investor Presentations
     +
Financial Filings
       ↓
Document Processing
       ↓
AI Search
       ↓
Financial Document Agent
       ↓
Question
       ↓
Evidence-Based Answer
```

Example:

> **"What were the company's major capital expenditures during the reporting period?"**

---

# 12. 🏦 M&A Research Agent

```text id="ib12"
Target Companies
      +
Financials
      +
Industry
      +
Transactions
      +
Comparable Companies
      ↓
M&A Research Agent
      ↓
Target Analysis
      ↓
Comparable Transactions
      ↓
Deal Intelligence
```

---

# 13. 🤝 Due Diligence Agent

```text id="ib13"
Financial Documents
      +
Contracts
      +
Operational Data
      +
Customer Data
      +
Legal Documents
      ↓
Due Diligence Agent
      ↓
Document Analysis
      ↓
Risk Identification
      ↓
Evidence Summary
```

---

# 14. 💰 Wealth Advisory Agent

```text id="ib14"
Client 360
    +
Portfolio
    +
Objectives
    +
Risk Profile
    +
Historical Performance
      ↓
Wealth Advisory Agent
      ↓
Portfolio Insights
      ↓
Scenario Analysis
      ↓
Advisor Review
```

---

# 15. 👤 Wealth Client 360 Agent

```text id="ib15"
Client
 ↓
Accounts
 +
Holdings
 +
Transactions
 +
Goals
 +
Risk Profile
 +
Interactions
      ↓
Client 360
      ↓
Wealth Intelligence Agent
```

---

# 16. 📜 Regulatory Compliance Agent

```text id="ib16"
Regulations
     +
Policies
     +
Controls
     +
Transactions
     +
Audit Evidence
      ↓
Compliance Agent
      ↓
Requirement Retrieval
      ↓
Control Mapping
      ↓
Gap Analysis
```

---

# 17. 🛡️ AML Investigation Agent

A strong **Graph + Streaming + RAG** project.

```text id="ib17"
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
Transaction Graph
      ↓
Anomaly Detection
      ↓
AML Agent
      ↓
Case Investigation
      ↓
Evidence Summary
      ↓
Compliance Review
```

Example:

> **"Identify unusual transaction relationships that require investigator review."**

---

# 18. 💳 Transaction Intelligence Agent

```text id="ib18"
Trade / Investment Transaction
          ↓
Execution
          ↓
Settlement
          ↓
Reconciliation
          ↓
Transaction Intelligence Agent
          ↓
Exception Detection
          ↓
Investigation
```

---

# 19. 📊 Performance Attribution Agent

```text id="ib19"
Portfolio Returns
      +
Holdings
      +
Sectors
      +
Asset Classes
      +
Benchmarks
      ↓
Performance Attribution Agent
      ↓
Return Attribution
      ↓
 ┌──────────┬──────────┬──────────┐
 ↓          ↓          ↓
Asset      Sector     Geography
      ↓
Performance Explanation
```

Example:

> **"Why did the portfolio underperform its benchmark this quarter?"**

---

# 🤖 Ultimate Capstone: Investment Operations Supervisor

```text id="ib20"
                    ┌───────────────────────────┐
                    │ Investment Operations      │
                    │     Supervisor Agent      │
                    └─────────────┬─────────────┘
                                  │
       ┌────────────┬─────────────┼──────────────┬──────────────┐
       ↓            ↓             ↓              ↓              ↓
 Research       Portfolio       Risk          Compliance     Transaction
 Agent           Agent          Agent            Agent          Agent
       ↓            ↓             ↓              ↓              ↓
Company         Holdings       Exposure        Regulatory      Settlement
Research        Analysis       Analysis        Analysis        Analysis
       │            │             │              │              │
       └────────────┴─────────────┼──────────────┴──────────────┘
                                  ↓
                         Market Intelligence
                                  ↓
                         Performance Agent
                                  ↓
                         Client 360 Agent
                                  ↓
                           Human Approval
```

# 🏦 Databricks Architecture

```text id="ibarch"
                  FINANCIAL DATA
                        │
       ┌────────────────┼─────────────────┐
       ↓                ↓                 ↓
 Market Data        Client Data       Documents
       ↓                ↓                 ↓
Prices              Accounts          Filings
Volumes             Holdings          Reports
News                Transactions      Contracts
Events              Risk Profile      Research
       │                │                 │
       └────────────────┼─────────────────┘
                        ↓
               Kafka / APIs / Files
                        ↓
          Databricks Structured Streaming
                        ↓
                   Delta Lake
                        ↓
                Unity Catalog
                        ↓
                 AI Agent Layer
                        ↓
       ┌────────────────┼────────────────┐
       ↓                ↓                ↓
    RAG Agent        SQL Agent       Tool Agent
       ↓                ↓                ↓
       └────────────────┼────────────────┘
                        ↓
                 Multi-Agent Layer
                        ↓
            Investment Supervisor
                        ↓
                Human-in-the-Loop
                        ↓
                 Databricks App
```

## 🎓 Student Progression

| Level               | Projects                                                                            | Skills                                            |
| ------------------- | ----------------------------------------------------------------------------------- | ------------------------------------------------- |
| 🟢 **Beginner**     | Investment Research, Financial Documents, Company Intelligence, Financial Analytics | RAG + SQL                                         |
| 🟡 **Intermediate** | Portfolio, Wealth 360, M&A Research, Due Diligence, Performance                     | RAG + Financial Analytics                         |
| 🟠 **Advanced**     | Market Events, Forecasting, Portfolio Risk, AML                                     | Streaming + ML + Graph                            |
| 🔴 **Expert**       | Asset Allocation, Financial Modeling, Transaction Intelligence                      | Optimization + Tool Calling                       |
| 🏆 **Capstone**     | **Investment Operations Supervisor**                                                | Multi-Agent + RAG + Streaming + Human-in-the-Loop |

### Ideal learning sequence

**Financial Research → Financial RAG → Text-to-SQL → Company 360 → Portfolio Agent → Risk Agent → Market News Agent → Real-Time Event Agent → AML Graph Agent → Multi-Agent Investment Supervisor**

 
