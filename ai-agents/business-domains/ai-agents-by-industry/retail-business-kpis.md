**Retail → E-Commerce → Quick Commerce Business**  
**FDE Requirements, KPIs & Use Cases**

---

### Major Business Model Types

| Model | Description | Examples |
|-------|-------------|----------|
| **B2B** | Business to Business – Manufacturer to Retailer / Wholesaler to Distributor | Brand → Distributor → Retailer |
| **B2C** | Business to Consumer | Walmart, Amazon Retail, Reliance Digital |
| **D2C** | Direct to Consumer – Own brand selling offline + online | Lenskart, Mamaearth |
| **B2B2C** | Business to Business to Consumer – Brand → Retailer → End Customer | Brand-led retail models |
| **Marketplace** | Platform model with multiple sellers | Amazon, Flipkart |
| **B2G** | Business to Government | Institutional / tender-based supply |
| **C2C** | Consumer to Consumer | OLX, eBay, Facebook & Instagram Marketplace |
| **C2B** | Consumer to Business | Freelancing, content creation, influencer marketing |

---

### Core Retail Business Lifecycles

1. **Procure to Pay (P2P)**  
   Retailer places Purchase Order → Supplier receives PO → Goods delivered to retailer.

2. **Order to Cash (O2C)**  
   Customer places order → Payment → Fulfillment → Delivery → Settlement.

3. **Inventory to Replenishment**  
   Central / city warehouses feed multiple retail stores or dark stores based on demand signals. Includes own inventory, own shipment, and inventory management systems with store-level tracking.

**Quick Commerce Specific Lifecycle**  
Order → Allocate Dark Store → Pick → Pack → Assign Rider → Dispatch → Deliver → Reconcile

---

### Agentic AI Engineer Focus Areas

- Sales Order AI Agent  
- Revenue AI Agent  
- Inventory AI Agent  
- Purchase Order (PO) AI Agent – volume of POs (monthly/weekly), on-time delivery, delays, damage, RAG on terms & conditions, invoice differences  
- Consumer / Customer Tracking AI Agent – returns, replacements, behavior

---

### FDE (Forward Deployed Engineer) Focus

FDEs must deeply understand end-to-end retail business processes so they can:

- Identify gaps in the current business model  
- Define KPIs that close those gaps  
- Propose KPIs / use cases that **increase revenue**  
- Propose KPIs / use cases that **reduce expenses**  
- Propose KPIs that help **scale** the business (more sales, more stores, higher AOV, better margins)  
- Quantify AI investment required vs expected ROI  
- Make the entire retail operation **AI-ready** – automate manual processes and instrument KPIs with AI  
- Incorporate external signals: local festivals, long weekends, city/state holidays, social data, calendar data → feed into AI for smarter PO placement, inventory positioning, and store/dark-store stocking  
- Deliver **quick wins** and clear implementation roadmaps

**Key External Intelligence Layer**  
- City / state-level festival & holiday calendar  
- Past sales history + demand forecasting  
- Social listening / regional trends  
- Automated email / WhatsApp notifications to regional, PO, and inventory teams

---

### Lifecycle → Common Gaps → FDE / AI Opportunities

| Lifecycle Stage | Common Business Gaps | FDE / AI Opportunities & KPIs |
|-----------------|----------------------|-------------------------------|
| **Plan to Forecast** | Incorrect demand forecasts, seasonal shortages | Demand forecasting, festival intelligence, promotional lift prediction |
| **Source to Contract** | Poor supplier pricing, contract leakage | Supplier intelligence, contract RAG, price benchmarking |
| **Procure to Pay (P2P)** | Late POs, invoice mismatches, damaged goods | PO monitoring, three-way match automation, supplier scoring |
| **Receive to Stock** | Inaccurate inventory, receiving delays | Inventory reconciliation, anomaly detection, GRN automation |
| **Replenish to Shelf / Dark Store** | Stockouts, overstock, wrong store allocation | Dynamic replenishment, transfer recommendations, real-time stock health |
| **Browse to Order** | Cart abandonment, low conversion | Search optimization, personalized recommendations, session intelligence |
| **Order to Cash (O2C)** | Payment failures, delayed delivery, cancellations | Order exception detection, payment recovery, delivery ETA accuracy |
| **Return to Resolution** | Refund delays, excessive returns | Returns intelligence, policy-aware assistance, reverse logistics optimization |
| **Customer Lifecycle** | Churn, low repeat purchases | Customer 360, next-best-action, retention campaigns |
| **Record to Report** | Unreconciled revenue, unknown margins | Finance reconciliation, profitability intelligence, real-time P&L |

---

### Expanded KPIs & Use Cases for FDEs

#### 1. Revenue Growth & Conversion
- Conversion rate (browse → cart → order)  
- Average Order Value (AOV) and AOV uplift from recommendations  
- Attach rate / cross-sell & up-sell rate  
- Cart abandonment rate + recovery rate  
- Revenue per visitor / per session  
- Festival / event-driven revenue uplift vs baseline  
- New vs repeat customer revenue mix

**Use Cases**  
- Personalized product recommendations + WhatsApp automation  
- Dynamic pricing / promotional suggestions based on inventory + demand  
- Festival-aware assortment and marketing pushes

#### 2. Inventory & Availability
- Stockout rate (by SKU, category, store/dark store)  
- Overstock / aged inventory %  
- Inventory turns / days of cover  
- Forecast accuracy (MAPE, bias)  
- Fill rate / perfect order rate  
- Dark-store utilization & allocation accuracy  
- Transfer recommendation acceptance rate

**Use Cases**  
- Real-time stockout monitoring + auto-replenishment triggers  
- Festival & long-weekend demand sensing → proactive PO & allocation  
- Anomaly detection on receiving vs expected

#### 3. Purchase & Supplier Performance (P2P)
- On-time delivery %  
- PO cycle time (creation → receipt)  
- Invoice match rate (3-way match)  
- Damage / shortage rate on receipt  
- Supplier lead-time adherence  
- Price variance vs contracted / market  
- Number of POs per week/month by category / region

**Use Cases**  
- PO AI Agent with SLA monitoring and exception alerts  
- Supplier scoring & intelligent reordering  
- Contract RAG + invoice discrepancy detection  
- Automated supplier discovery & margin negotiation support

#### 4. Fulfillment & Quick Commerce Operations
- Order-to-delivery time (OTD)  
- On-time delivery %  
- Pick accuracy / pick rate  
- Rider assignment latency & utilization  
- Delivery success rate (first attempt)  
- Cost per order / cost per delivery  
- Dark-store order allocation accuracy

**Use Cases**  
- Real-time order exception detection & recovery  
- Dynamic dark-store allocation + rider matching  
- ETA prediction improvement using live signals

#### 5. Customer Experience & Retention
- Repeat purchase rate / purchase frequency  
- Customer Lifetime Value (CLV / LTV)  
- Churn rate (30/60/90-day inactivity)  
- Net Promoter Score / CSAT  
- Return rate & reason codes  
- Time-to-refund / resolution time  
- Customer 360 completeness score

**Use Cases**  
- Customer 360 + next-best-action (product + offer)  
- 30+ day inactivity triggers → personalized discount / re-engagement via WhatsApp  
- Returns intelligence & policy-aware chatbot / agent  
- Region + festival-aware personalized offers

#### 6. Margin & Cost Control
- Gross margin % by category / SKU / channel  
- Contribution margin after delivery & returns  
- Shrinkage / wastage %  
- Cost of goods sold (COGS) variance  
- Logistics cost as % of revenue  
- AI-driven cost avoidance (automation hours saved, exception reduction)

**Use Cases**  
- Margin leakage detection (pricing, promotions, returns, supplier)  
- Automated three-way match & invoice recovery  
- Route / rider optimization for cost reduction

#### 7. Scalability & Operational Efficiency
- Orders processed per FTE / per dark store  
- Automation coverage % (manual vs AI-handled tasks)  
- Time-to-insight for key decisions (forecast, PO, allocation)  
- Store / dark-store expansion readiness score  
- Forecast-driven capacity planning accuracy

---

### High-Impact Quick Wins for FDEs to Showcase

1. **Festival & Holiday Intelligence Pipeline**  
   City/state calendar + past sales → automated recommendations for PO volume, inventory positioning, and regional notifications.

2. **Stockout & Overstock Real-time Monitoring**  
   Live alerts + suggested transfers or emergency POs.

3. **Customer 360 + WhatsApp Re-engagement**  
   Inactivity detection → personalized offers → measurable retention lift.

4. **PO & Invoice Exception Agent**  
   Auto-detect delays, mismatches, damage → route to correct team with context.

5. **Supplier Intelligence Layer**  
   Public data + historical performance → better pricing and reliability recommendations.

6. **Returns Root-Cause Dashboard**  
   Pattern detection by SKU, region, reason → policy or quality actions.

---

### FDE Operating Model Recommendation

- Map current process → identify data gaps and manual bottlenecks  
- Prioritize use cases by **Revenue Impact × Feasibility × Data Readiness**  
- Instrument KPIs first, then layer agents  
- Build closed-loop systems (detect → recommend → act → measure)  
- Maintain a living **AI Opportunity Backlog** tied to business outcomes  
- Quantify investment (data engineering, agent development, integration, change management) against projected uplift in revenue, margin, and cost reduction

---

This framework gives FDEs a clear, prioritized view of where AI creates measurable value across traditional retail, e-commerce, and quick-commerce operations.
