# O2C Graph Intelligence

> Order-to-Cash Graph Explorer with Natural Language Query Interface

A full-stack application that transforms fragmented SAP Order-to-Cash data into an interactive graph, allowing users to explore entity relationships visually and query the data using natural language.

---

## What It Does

- Ingests SAP O2C data (JSONL format) and loads it into a structured SQLite database
- Constructs an in-memory graph using NetworkX to model entity relationships
- Visualizes the graph as a force-directed network using D3.js
- Provides a chat interface where users ask questions in natural language
- Translates natural language into SQL dynamically using Groq (Llama 3.3 70B)
- Returns data-backed answers grounded in the actual dataset
- Highlights and pans to relevant graph nodes when queries return results

---

## The O2C Flow

```
Business Partner (Customer)
        │
        ▼
Sales Order ──── Sales Order Items ──── Products
        │
        ▼
Delivery ──── Delivery Items ──── Plants
        │
        ▼
Billing Document ──── Billing Items
        │
        ▼
Journal Entry
        │
        ▼
Payment
```

---

## Tech Stack

| Layer | Technology | Reason |
|-------|-----------|--------|
| Backend | Python + FastAPI | Lightweight, fast API with automatic docs |
| Database | SQLite | Zero setup, single file, perfect for this dataset size |
| Graph (in-memory) | NetworkX | Graph construction and traversal without a graph DB |
| LLM | Groq (Llama 3.3 70B) | Free tier, fastest inference, excellent SQL generation |
| Graph Visualization | D3.js (force-directed) | Full control over layout, interactions, and animations |
| Frontend | React | Component-based UI, easy state management |

---

## Architecture Decisions

### Why SQLite over Neo4j or PostgreSQL

The dataset is small (under 20,000 rows total) and the primary query pattern is relational joins, not graph traversals. SQLite requires zero infrastructure, deploys as a single file, and LLMs generate standard SQL against it reliably. Neo4j would add operational complexity and require learning Cypher — the added cost outweighs any benefit at this data scale.

### Why NetworkX instead of a graph database

The graph is used purely for visualization, not for querying. NetworkX constructs the graph in-memory at startup from SQLite data, which is fast and simple. There is no need for persistent graph storage.

### Why Groq (Llama 3.3 70B)

- Free tier with 14,400 requests/day
- Sub-second inference speed
- Llama 3.3 70B generates accurate, complex SQL with multi-table joins
- OpenAI-compatible API — minimal code changes needed

### Why D3.js for the graph

React Flow was evaluated but D3's force simulation gives more natural, organic layouts that match how real graph tools look. D3 also provides fine-grained control over node sizing, edge rendering, zoom, pan, and animations.

---

## NL to SQL Prompting Strategy

The system uses a two-step LLM pipeline:

**Step 1 — Question to SQL**

The LLM receives a detailed system prompt containing:
- All table names and column names
- Key join relationships between tables
- Explicit rules for join patterns (e.g. never join salesOrder directly to billingDocument)
- An instruction to return `UNRELATED_QUERY` for off-topic questions
- A rule to always use `LIKE '%value%'` for name searches

```
System: You are a SQL generator for an SAP O2C dataset...
        [full schema]
        [join rules]
        Rules: return raw SQL only, no markdown, no explanation
               return UNRELATED_QUERY if question is off-topic

User:   Which products are associated with the highest number of billing documents?

Model:  SELECT material, COUNT(billingDocument) AS num_billing_docs
        FROM sales_order_items ...
```

**Step 2 — Results to Answer**

The SQL is executed against SQLite. Results (capped at 20 rows) are passed back to the LLM with the original question to generate a natural language answer grounded in the data.

---

## Guardrails

Two layers of guardrail protection:

**Layer 1 — LLM level**

The system prompt explicitly instructs the model to return `UNRELATED_QUERY` for anything outside the dataset domain. This catches general knowledge questions, creative writing requests, and personal queries.

**Layer 2 — Code level**

The backend checks for `UNRELATED_QUERY` before executing any SQL and returns a fixed message. SQL execution is wrapped in try/catch — errors return a graceful message without exposing internals.

Example rejected queries:
- "What is the capital of France?" → rejected
- "Write me a poem" → rejected  
- "Who is Elon Musk?" → rejected

---

## Dataset

The dataset is an SAP Order-to-Cash simulation with the following entities:

| Table | Rows | Description |
|-------|------|-------------|
| sales_order_headers | 100 | Sales order master data |
| sales_order_items | 167 | Line items per sales order |
| delivery_headers | 86 | Outbound delivery headers |
| delivery_items | 137 | Delivery line items |
| billing_headers | 163 | Billing document headers |
| billing_items | 245 | Billing line items |
| journal_entries | 123 | Accounting postings |
| payments | 120 | Payment records |
| business_partners | 8 | Customer master data |
| products | 69 | Product master data |
| plants | 44 | Plant master data |

---

## Project Structure

```
o2c-graph-intelligence/
├── backend/
│   ├── main.py           # FastAPI app — all API endpoints
│   ├── database.py       # JSONL loader → SQLite
│   ├── llm.py            # NL → SQL → Answer pipeline
│   ├── graph.py          # NetworkX graph construction
│   ├── data/
│   │   └── business.db   # SQLite database (generated, not committed)
│   ├── .env              # API keys (not committed)
│   ├── .env.example      # Key template (committed)
│   └── requirements.txt
├── frontend/
│   ├── src/
│   │   ├── App.js        # Root component, layout
│   │   ├── GraphView.jsx # D3 force-directed graph
│   │   ├── ChatPanel.jsx # Chat interface
│   │   └── App.css       # Global styles
│   └── package.json
├── .gitignore
└── README.md
```

---

## Local Setup

### Prerequisites
- Python 3.10+
- Node.js 18+
- Groq API key (free at console.groq.com)

### Backend

```bash
cd backend
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt

# Add your Groq API key
cp .env.example .env
# Edit .env and add: GROQ_API_KEY=your_key_here

# Load the dataset into SQLite
python database.py

# Start the API server
python main.py
# Server runs at http://localhost:8000
```

### Frontend

```bash
cd frontend
npm install
npm start
# App runs at http://localhost:3000
```

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | /query | Natural language query → SQL → answer |
| GET | /graph/overview | Entity counts summary |
| GET | /graph/nodes | All nodes and edges for visualization |
| GET | /graph/node/{id} | Full details for a specific node |

---

## Example Queries

```
Which products are associated with the highest number of billing documents?
Which sales orders were delivered but never billed?
Which customers have the most incomplete order flows?
Trace the full flow of billing document 90504204
Find all links to customer Nguyen-Davis
How many payments has customer 320000083 made?
```

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| GROQ_API_KEY | Your Groq API key from console.groq.com |

---

| Item | Reason |
|------|--------|
| `backend/data/*.db` | Generated file — run database.py to recreate (Although already in repo)|

---

## What Is Not Committed to GitHub

| Item | Reason |
|------|--------|
| `.env` | Contains API keys |
| `node_modules/` | Installed via npm install |
| `__pycache__/` | Python cache |
