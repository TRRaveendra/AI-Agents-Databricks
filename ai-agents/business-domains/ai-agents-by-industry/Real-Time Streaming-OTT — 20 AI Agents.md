# 📺    Real-Time Streaming / OTT — 20 AI Agents

This domain is especially valuable for your students because it combines **real-time streaming, video QoE, CDN telemetry, viewer analytics, content intelligence, personalization, advertising, subscriptions, churn, content rights, and AI-powered operations**.

|  # | AI Agent                                     | What the Agent Does                                                                          | Key Databricks Capability       |
| -: | -------------------------------------------- | -------------------------------------------------------------------------------------------- | ------------------------------- |
|  1 | 🎬 **Content Recommendation Agent**          | Recommends movies, shows, live events and videos based on viewer behavior                    | Recommendation + Customer 360   |
|  2 | 👤 **Viewer 360 Intelligence Agent**         | Creates a unified profile of viewer behavior, subscriptions, devices and content preferences | Customer 360 + SQL              |
|  3 | 📺 **Streaming Quality Agent**               | Monitors buffering, startup time, bitrate, resolution and playback failures                  | Streaming + Time Series         |
|  4 | 🌐 **CDN Performance Agent**                 | Monitors CDN nodes, latency, throughput, cache hit ratio and regional performance            | Streaming + Observability       |
|  5 | 🚨 **Streaming Incident Agent**              | Detects and investigates real-time streaming incidents and service degradation               | Streaming + Anomaly Detection   |
|  6 | 🔍 **Streaming Root Cause Agent**            | Correlates CDN, network, application and player telemetry to identify likely root causes     | SQL + RAG + Tool Calling        |
|  7 | 📉 **Viewer Churn Prediction Agent**         | Identifies subscribers likely to cancel or reduce usage                                      | ML + Customer 360               |
|  8 | 💳 **Subscription Intelligence Agent**       | Analyzes plans, upgrades, downgrades, renewals and subscription behavior                     | SQL + Analytics                 |
|  9 | 🎯 **Personalization Agent**                 | Personalizes home pages, content rows, notifications and viewer experiences                  | Recommendation + ML             |
| 10 | 📊 **Audience Analytics Agent**              | Analyzes audience size, engagement, viewing duration and content performance                 | SQL + BI                        |
| 11 | 🔥 **Live Event Monitoring Agent**           | Monitors live sports, concerts, news and other high-volume streaming events                  | Streaming + Real-Time Analytics |
| 12 | 📈 **Content Performance Agent**             | Measures content engagement, completion rates, retention and audience reach                  | SQL + Time Series               |
| 13 | 📝 **Content Intelligence Agent**            | Analyzes scripts, metadata, reviews, subtitles and audience feedback                         | RAG + NLP                       |
| 14 | 🛡️ **Content Piracy Detection Agent**       | Detects suspicious streaming, account-sharing and content-abuse patterns                     | Streaming + Anomaly Detection   |
| 15 | 📜 **Content Rights Management Agent**       | Tracks licensing terms, territories, expiration dates and content rights                     | Document AI + RAG               |
| 16 | 💰 **Advertising Intelligence Agent**        | Analyzes ad impressions, engagement, fill rates and campaign performance                     | Streaming + Analytics           |
| 17 | 🎯 **Ad Personalization Agent**              | Recommends relevant advertising segments based on viewer and content context                 | ML + Customer 360               |
| 18 | ⚙️ **Streaming Capacity Planning Agent**     | Forecasts traffic and infrastructure requirements for future streaming demand                | Time Series + Forecasting       |
| 19 | 💬 **Viewer Support Agent**                  | Handles playback, subscription, account and content-related support requests                 | RAG + Tool Calling              |
| 20 | 🤖 **Streaming Operations Supervisor Agent** | Coordinates viewer, content, CDN, streaming quality, advertising and subscription agents     | Multi-Agent + Supervisor        |

---

# ⭐ Top Student Projects

## 1. 🎬 Content Recommendation Agent

```text id="ott01"
Viewer
   ↓
Viewing History
   +
Search
   +
Watch Time
   +
Ratings
   +
Similar Viewers
      ↓
Recommendation Agent
      ↓
Candidate Content
      ↓
Ranking
      ↓
Personalized Recommendations
```

Example:

> **"What content should we recommend to this viewer next?"**

---

# 2. 👤 Viewer 360 Intelligence Agent

```text id="ott02"
Viewer
 ↓
 ┌──────────┬──────────┬──────────┬──────────┐
 ↓          ↓          ↓          ↓
Viewing    Search    Subscription Device
History    History    History     History
 ↓          ↓          ↓          ↓
Interactions
      ↓
Viewer 360
      ↓
Viewer Intelligence Agent
```

---

# 3. 📺 Streaming Quality Agent

One of the strongest **real-time Databricks projects**.

```text id="ott03"
Video Player
    ↓
Playback Events
    +
Buffering
    +
Startup Time
    +
Bitrate
    +
Resolution
    +
Errors
       ↓
Kafka
       ↓
Databricks Structured Streaming
       ↓
Streaming Quality Agent
       ↓
QoE Score
       ↓
Anomaly Detection
```

Example:

> **"Why did streaming quality deteriorate for viewers in Region A?"**

---

# 4. 🌐 CDN Performance Agent

```text id="ott04"
CDN Nodes
    +
Cache Hit Ratio
    +
Latency
    +
Throughput
    +
Traffic
      ↓
CDN Agent
      ↓
Regional Analysis
      ↓
Performance Anomaly
      ↓
Capacity / Routing Recommendation
```

---

# 5. 🚨 Streaming Incident Agent

```text id="ott05"
Streaming Events
      +
CDN Metrics
      +
Application Logs
      +
Network Metrics
      +
Player Errors
      ↓
Incident Agent
      ↓
Incident Detection
      ↓
Severity
      ↓
Affected Regions
      ↓
Incident Summary
```

---

# 6. 🔍 Streaming Root Cause Agent

```text id="ott06"
Streaming Incident
       ↓
 ┌─────────┬─────────┬──────────┬─────────┐
 ↓         ↓         ↓          ↓
CDN       Network   Player     Application
Metrics   Metrics   Metrics      Logs
       ↓
Root Cause Agent
       ↓
Correlation
       ↓
Likely Root Cause
       ↓
Recommended Action
```

This is an excellent **Agentic RCA** project.

---

# 7. 📉 Viewer Churn Prediction Agent

```text id="ott07"
Viewing
 +
Session Duration
 +
Content Engagement
 +
Subscription
 +
Support Issues
 +
Playback Problems
       ↓
Churn Model
       ↓
Churn Agent
       ↓
Risk Score
       ↓
Churn Drivers
       ↓
Retention Recommendation
```

---

# 8. 💳 Subscription Intelligence Agent

```text id="ott08"
Subscriptions
     +
Plans
     +
Upgrades
     +
Downgrades
     +
Payments
     +
Usage
       ↓
Subscription Agent
       ↓
Customer Analysis
       ↓
Upgrade / Renewal / Churn Insight
```

---

# 9. 🎯 Personalization Agent

```text id="ott09"
Viewer 360
    +
Content Preferences
    +
Current Session
    +
Device
    +
Time
    +
Location
      ↓
Personalization Agent
      ↓
Homepage
Content Rows
Notifications
Offers
```

---

# 10. 📊 Audience Analytics Agent

A strong **Text-to-SQL** project.

```text id="ott10"
Business Question
       ↓
Audience Analytics Agent
       ↓
Generate SQL
       ↓
Databricks SQL
       ↓
Streaming Lakehouse
       ↓
Audience Insights
```

Example:

> **"What percentage of viewers watched more than 80% of the new series?"**

---

# 11. 🔥 Live Event Monitoring Agent

Perfect for **sports/event streaming**.

```text id="ott11"
Live Event
    ↓
Viewer Events
    +
Traffic
    +
CDN
    +
Buffering
    +
Errors
    +
Concurrent Viewers
       ↓
Streaming
       ↓
Live Event Agent
       ↓
Real-Time Event Health
       ↓
Alert / Recommendation
```

---

# 12. 📈 Content Performance Agent

```text id="ott12"
Content
   ↓
Views
   +
Watch Time
   +
Completion
   +
Engagement
   +
Search
   +
Shares
      ↓
Content Performance Agent
      ↓
Content Score
      ↓
Audience Insights
      ↓
Programming Recommendation
```

---

# 13. 📝 Content Intelligence Agent

```text id="ott13"
Scripts
  +
Metadata
  +
Subtitles
  +
Reviews
  +
Social Feedback
      ↓
Content Intelligence Agent
      ↓
Topics
Sentiment
Themes
Characters
      ↓
Content Insights
```

---

# 14. 🛡️ Content Piracy Detection Agent

```text id="ott14"
Viewer Sessions
      +
Account Sharing
      +
Device Patterns
      +
Geographic Activity
      +
Playback Behavior
      ↓
Piracy Detection Agent
      ↓
Anomaly Detection
      ↓
Risk Score
      ↓
Investigation
```

---

# 15. 📜 Content Rights Management Agent

Excellent **Document AI + RAG** project.

```text id="ott15"
Licensing Contracts
      +
Rights Agreements
      +
Content Metadata
      ↓
Document AI
      ↓
Rights Agent
      ↓
 ┌──────────┬──────────┬──────────┐
 ↓          ↓          ↓
Territory  Expiration  Restrictions
      ↓
Rights Intelligence
```

Example:

> **"Which content licenses expire within the next 90 days?"**

---

# 16. 💰 Advertising Intelligence Agent

```text id="ott16"
Ad Impressions
     +
Views
     +
Clicks
     +
Completion
     +
Audience
     +
Campaign
       ↓
Advertising Agent
       ↓
Campaign Performance
       ↓
Fill Rate
       ↓
Revenue Insights
```

---

# 17. 🎯 Ad Personalization Agent

```text id="ott17"
Viewer 360
     +
Content Context
     +
Audience Segment
     +
Ad History
     +
Campaign Rules
       ↓
Ad Personalization Agent
       ↓
Audience Matching
       ↓
Ad Recommendation
```

---

# 18. ⚙️ Streaming Capacity Planning Agent

```text id="ott18"
Historical Traffic
       +
Concurrent Viewers
       +
Live Events
       +
Regional Demand
       ↓
Forecasting
       ↓
Capacity Agent
       ↓
Traffic Forecast
       ↓
Infrastructure Requirement
```

---

# 19. 💬 Viewer Support Agent

```text id="ott19"
Viewer Question
       ↓
Viewer Support Agent
       ↓
 ┌──────────┬────────────┬─────────────┐
 ↓          ↓            ↓
Playback   Account      Subscription
 ↓          ↓            ↓
Content    Billing      Device
       ↓
RAG + Tool Calling
       ↓
Personalized Response
```

Example:

> **"Why is this video buffering only on my TV?"**

---

# 🤖 Ultimate Capstone: Streaming Operations Supervisor

```text id="ott20"
                       ┌───────────────────────────┐
                       │ Streaming Operations      │
                       │      Supervisor Agent     │
                       └─────────────┬─────────────┘
                                     │
       ┌────────────┬────────────────┼──────────────┬──────────────┐
       ↓            ↓                ↓              ↓              ↓
 Viewer Agent   Content Agent    Quality Agent   CDN Agent     Subscription
                                                                  Agent
       ↓            ↓                ↓              ↓              ↓
 Viewer 360      Content         QoE Analysis    Network        Churn
 Recommendation  Intelligence                    Performance     Analysis
       │            │                │              │              │
       └────────────┴────────────────┼──────────────┴──────────────┘
                                     ↓
                              Incident Agent
                                     ↓
                            Root Cause Agent
                                     ↓
                         Capacity Planning Agent
                                     ↓
                              Human Approval
```

# 📺 Databricks Streaming Architecture

```text
                   STREAMING DATA
                         │
       ┌─────────────────┼─────────────────┐
       ↓                 ↓                 ↓
    Viewer Data       Content Data       Platform
       ↓                 ↓                 ↓
 Watch Events         Metadata           CDN
 Search               Catalog            Network
 Playback             Rights             Servers
 Clicks               Reviews            Player Logs
 Subscriptions        Ads                QoE
       │                 │                 │
       └─────────────────┼─────────────────┘
                         ↓
                    Kafka / APIs
                         ↓
             Databricks Structured Streaming
                         ↓
                    Delta Lake
                         ↓
                 Unity Catalog
                         ↓
                   Viewer 360
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
              Streaming Supervisor
                         ↓
                  Human-in-the-Loop
                         ↓
                  Databricks App
```

## 🎓 Student Progression

| Level               | Projects                                                                     | Skills                                      |
| ------------------- | ---------------------------------------------------------------------------- | ------------------------------------------- |
| 🟢 **Beginner**     | Viewer Support, Content Intelligence, Rights, Audience Analytics, Viewer 360 | RAG + SQL                                   |
| 🟡 **Intermediate** | Recommendation, Subscription, Advertising, Content Performance               | Customer 360 + ML                           |
| 🟠 **Advanced**     | Streaming Quality, CDN, Live Events, Viewer Churn                            | Streaming + Time Series                     |
| 🔴 **Expert**       | Root Cause, Capacity Planning, Piracy, Ad Personalization                    | Real-Time + Optimization                    |
| 🏆 **Capstone**     | **Streaming Operations Supervisor**                                          | Multi-Agent + Real-Time + Human-in-the-Loop |

### Ideal learning sequence

**Viewer Support → Streaming RAG → SQL Analytics → Viewer 360 → Recommendation → Churn → Content Intelligence → Real-Time QoE → CDN/Incident RCA → Multi-Agent Streaming Supervisor**

 
