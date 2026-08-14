# 👥   Human Resources / HR — 20 AI Agents

HR is a strong **Databricks Agentic AI** domain because it combines **employee 360, recruitment, resumes, workforce analytics, payroll, learning, performance, attrition, skills, compensation, employee support, and compliance**.

|  # | AI Agent                                   | What the Agent Does                                                                                   | Key Databricks Capability   |
| -: | ------------------------------------------ | ----------------------------------------------------------------------------------------------------- | --------------------------- |
|  1 | 👤 **Employee 360 Agent**                  | Creates a unified view of employee profile, role, skills, performance, learning and career history    | Customer/Employee 360 + SQL |
|  2 | 📄 **Resume Screening Agent**              | Extracts candidate skills, experience and qualifications and compares them with job requirements      | Document AI + RAG           |
|  3 | 🎯 **Candidate Matching Agent**            | Matches candidates to open positions based on skills and job requirements                             | Embeddings + Recommendation |
|  4 | 🔎 **Recruitment Research Agent**          | Researches candidates, roles, skills and market talent information                                    | RAG + AI Search             |
|  5 | 📅 **Interview Scheduling Agent**          | Coordinates interviewer availability and candidate scheduling                                         | Tool Calling + APIs         |
|  6 | 📝 **Interview Intelligence Agent**        | Summarizes interviews and extracts skills, requirements and action items                              | NLP + RAG                   |
|  7 | 📊 **Recruitment Analytics Agent**         | Analyzes hiring funnel, time-to-hire, source effectiveness and recruiter performance                  | SQL + BI                    |
|  8 | 📉 **Employee Attrition Agent**            | Identifies workforce attrition patterns and potential organizational risk                             | ML + Analytics              |
|  9 | 🎓 **Learning Recommendation Agent**       | Recommends training based on employee skills, role and career goals                                   | Recommendation + ML         |
| 10 | 🧠 **Skills Intelligence Agent**           | Builds an organizational skills graph and identifies skill gaps                                       | Graph + Embeddings          |
| 11 | 🚀 **Career Development Agent**            | Recommends career paths, skills and learning opportunities                                            | RAG + Recommendation        |
| 12 | 📈 **Performance Intelligence Agent**      | Analyzes performance goals, reviews and achievements for management insights                          | SQL + Analytics             |
| 13 | 💰 **Compensation Intelligence Agent**     | Analyzes compensation structures, pay trends and internal equity for HR review                        | SQL + Analytics             |
| 14 | 🧾 **Payroll Intelligence Agent**          | Investigates payroll exceptions, discrepancies and employee payroll questions                         | SQL + Tool Calling          |
| 15 | 💬 **Employee Support Agent**              | Answers questions about HR policies, benefits, leave, payroll and company procedures                  | RAG + AI Search             |
| 16 | 📜 **HR Policy Compliance Agent**          | Searches HR policies and regulations and identifies compliance gaps                                   | RAG + AI Search             |
| 17 | 🏢 **Workforce Planning Agent**            | Forecasts workforce demand and identifies hiring/capacity requirements                                | Forecasting + Optimization  |
| 18 | 🌎 **Workforce Diversity Analytics Agent** | Provides workforce demographic and representation analytics with appropriate governance               | SQL + BI                    |
| 19 | 🧳 **Employee Onboarding Agent**           | Coordinates onboarding tasks, documents, training and access workflows                                | Agent + Workflow            |
| 20 | 🤖 **HR Operations Supervisor Agent**      | Coordinates recruitment, employee support, skills, learning, payroll, workforce and compliance agents | Multi-Agent + Supervisor    |

> **Important for student projects:** HR agents should use synthetic/de-identified employee data and should not make autonomous hiring, firing, promotion, compensation, or other high-impact employment decisions.

---

# ⭐ Top Student Projects

## 1. 👤 Employee 360 Agent

```text id="hr01"
Employee
   ↓
 ┌──────────┬──────────┬──────────┬──────────┐
 ↓          ↓          ↓          ↓
Profile   Skills    Performance Learning
 ↓          ↓          ↓          ↓
Career     Projects  Goals       Certifications
      ↓
Employee 360
      ↓
Employee Intelligence Agent
```

Example:

> **"Give me a complete summary of this employee's skills, projects and learning history."**

---

# 2. 📄 Resume Screening Agent

```text id="hr02"
Resume
  ↓
Document AI
  ↓
Skill Extraction
  ↓
Experience Extraction
  ↓
Job Description
  ↓
Resume Screening Agent
  ↓
Skill Match
  ↓
Recruiter Review
```

---

# 3. 🎯 Candidate Matching Agent

```text id="hr03"
Job Description
      +
Required Skills
      +
Candidate Profiles
      +
Experience
      ↓
Embeddings
      ↓
Candidate Matching Agent
      ↓
Candidate Ranking
      ↓
Recruiter Review
```

---

# 4. 🔎 Recruitment Research Agent

```text id="hr04"
Job Role
   +
Required Skills
   +
Market Data
   +
Candidate Information
      ↓
Recruitment Research Agent
      ↓
Talent Intelligence
      ↓
Recruiter Brief
```

---

# 5. 📅 Interview Scheduling Agent

```text id="hr05"
Candidate
    +
Interviewers
    +
Calendar Availability
    +
Interview Requirements
       ↓
Scheduling Agent
       ↓
Calendar API
       ↓
Available Slots
       ↓
Candidate Confirmation
```

Excellent **Tool Calling + API** project.

---

# 6. 📝 Interview Intelligence Agent

```text id="hr06"
Interview Transcript
       ↓
Interview Agent
       ↓
 ┌────────────┬────────────┬────────────┐
 ↓            ↓            ↓
Summary     Skills       Action Items
 ↓            ↓            ↓
Questions   Evidence     Follow-up
```

Use it as **structured interview documentation**, not autonomous candidate selection.

---

# 7. 📊 Recruitment Analytics Agent

```text id="hr07"
Applications
    +
Interviews
    +
Offers
    +
Hires
    +
Recruiters
      ↓
Recruitment Analytics Agent
      ↓
Hiring Funnel
      ↓
Time-to-Hire
      ↓
Source Effectiveness
```

Example:

> **"Which recruitment channel produces the highest qualified-candidate conversion?"**

---

# 8. 📉 Employee Attrition Agent

```text id="hr08"
Employee Data
      +
Tenure
      +
Role
      +
Engagement
      +
Learning
      +
Historical Attrition
      ↓
Attrition Model
      ↓
Attrition Agent
      ↓
Workforce Risk Insights
      ↓
HR Review
```

Avoid using sensitive personal attributes as decision variables.

---

# 9. 🎓 Learning Recommendation Agent

```text id="hr09"
Employee
   +
Current Skills
   +
Job Role
   +
Career Goal
   +
Training History
      ↓
Learning Agent
      ↓
Skill Gap
      ↓
Course Recommendation
      ↓
Learning Path
```

This is particularly useful for your **student-training / corporate-training** use cases.

---

# 10. 🧠 Skills Intelligence Agent

```text id="hr10"
Employees
   +
Resumes
   +
Projects
   +
Certifications
   +
Job Roles
   +
Training
      ↓
Skill Extraction
      ↓
Skills Graph
      ↓
Skills Intelligence Agent
      ↓
 ┌───────────┬───────────┐
 ↓           ↓
Skill Gap   Emerging Skills
```

This is an excellent **Graph + Agent** project.

---

# 11. 🚀 Career Development Agent

```text id="hr11"
Employee
   +
Current Skills
   +
Experience
   +
Target Role
   +
Available Training
      ↓
Career Agent
      ↓
Skill Gap
      ↓
Career Path
      ↓
Recommended Learning
```

---

# 12. 📈 Performance Intelligence Agent

```text id="hr12"
Goals
 +
Achievements
 +
Projects
 +
Feedback
 +
Performance Reviews
      ↓
Performance Agent
      ↓
Performance Trends
      ↓
Goal Progress
      ↓
Manager Insights
```

The agent should summarize evidence rather than autonomously determine promotions or ratings.

---

# 13. 💰 Compensation Intelligence Agent

```text id="hr13"
Employee Compensation
      +
Role
      +
Level
      +
Location
      +
Market Benchmarks
      ↓
Compensation Agent
      ↓
Pay Analysis
      ↓
Internal Equity Insights
      ↓
HR Review
```

---

# 14. 🧾 Payroll Intelligence Agent

```text id="hr14"
Payroll
   +
Attendance
   +
Benefits
   +
Deductions
   +
Employee Data
      ↓
Payroll Agent
      ↓
Exception Detection
      ↓
Discrepancy Analysis
      ↓
Payroll Team
```

Example:

> **"Identify payroll records where the calculated amount differs from the expected amount."**

---

# 15. 💬 Employee Support Agent

One of the best **RAG Agent** projects.

```text id="hr15"
Employee Question
       ↓
HR Support Agent
       ↓
HR Policies
   +
Benefits
   +
Leave Policy
   +
Payroll FAQ
   +
Company Handbook
       ↓
AI Search / RAG
       ↓
Evidence-Based Answer
```

Example:

> **"What is the company's leave policy for this type of absence?"**

---

# 16. 📜 HR Policy Compliance Agent

```text id="hr16"
HR Policies
    +
Regulations
    +
Company Procedures
    +
Audit Evidence
       ↓
Compliance Agent
       ↓
Policy Retrieval
       ↓
Control Mapping
       ↓
Gap Analysis
       ↓
HR Review
```

---

# 17. 🏢 Workforce Planning Agent

```text id="hr17"
Historical Workforce
       +
Business Growth
       +
Attrition
       +
Open Positions
       +
Productivity
       ↓
Forecasting
       ↓
Workforce Planning Agent
       ↓
Future Workforce Demand
       ↓
Hiring / Capacity Recommendation
```

---

# 18. 🌎 Workforce Analytics Agent

```text id="hr18"
Workforce Data
      +
Organizational Structure
      +
Locations
      +
Roles
      +
Representation Metrics
      ↓
Workforce Analytics Agent
      ↓
Workforce Insights
      ↓
Trend Analysis
      ↓
HR Reporting
```

For educational projects, apply appropriate **privacy, fairness and access controls**.

---

# 19. 🧳 Employee Onboarding Agent

```text id="hr19"
New Employee
      ↓
Onboarding Agent
      ↓
 ┌────────────┬────────────┬────────────┐
 ↓            ↓            ↓
Documents   Training    Access Requests
 ↓            ↓            ↓
Policies     Orientation  Systems
      ↓
Onboarding Checklist
      ↓
Completion Tracking
```

This is a good **workflow + tool-calling Agent**.

---

# 🤖 Ultimate Capstone: HR Operations Supervisor

```text id="hr20"
                         ┌────────────────────────┐
                         │ HR Operations           │
                         │    Supervisor Agent     │
                         └───────────┬────────────┘
                                     │
       ┌────────────┬────────────────┼──────────────┬──────────────┐
       ↓            ↓                ↓              ↓              ↓
Recruitment     Employee 360      Learning      Payroll        Support
   Agent           Agent           Agent         Agent          Agent
       ↓            ↓                ↓              ↓              ↓
Candidate        Employee         Skills        Exceptions      HR RAG
Matching         Intelligence     Gaps
       │            │                │              │              │
       └────────────┴────────────────┼──────────────┴──────────────┘
                                     ↓
                              Workforce Agent
                                     ↓
                             Compliance Agent
                                     ↓
                            Career Agent
                                     ↓
                              Human Approval
```

# 🏢 Databricks HR Architecture

```text
                      HR DATA
                         │
       ┌─────────────────┼─────────────────┐
       ↓                 ↓                 ↓
 Employee Data       Recruitment        Enterprise
       ↓                 ↓                 ↓
 Profiles            Resumes             Payroll
 Skills              Candidates          Benefits
 Performance         Interviews          Attendance
 Learning            Jobs                Contracts
 Projects            Offers              Policies
       │                 │                 │
       └─────────────────┼─────────────────┘
                         ↓
                    APIs / Files
                         ↓
                 Databricks Lakehouse
                         ↓
                    Delta Lake
                         ↓
                 Unity Catalog
                         ↓
                  Employee 360
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
                   HR Supervisor
                         ↓
                  Human-in-the-Loop
                         ↓
                  Databricks App
```

## 🎓 Student Progression

| Level               | Projects                                                                 | Skills                          |
| ------------------- | ------------------------------------------------------------------------ | ------------------------------- |
| 🟢 **Beginner**     | Employee Support, HR Analytics, Policy Agent, Resume Agent, Employee 360 | RAG + SQL                       |
| 🟡 **Intermediate** | Interview, Recruitment, Learning, Payroll, Onboarding                    | Document AI + Tool Calling      |
| 🟠 **Advanced**     | Attrition, Skills Intelligence, Workforce Planning, Performance          | ML + Graph + Analytics          |
| 🔴 **Expert**       | Career Intelligence, Compensation, Talent Intelligence, Multi-Agent HR   | Recommendation + Governance     |
| 🏆 **Capstone**     | **HR Operations Supervisor**                                             | Multi-Agent + Human-in-the-Loop |

### Ideal learning sequence

**HR Support → HR RAG → Employee 360 → Resume Agent → Candidate Matching → Recruitment Analytics → Skills Intelligence → Learning Recommendation → Workforce Planning → Multi-Agent HR Supervisor**

 
