from dotenv import load_dotenv
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import sqlite3
import networkx as nx
from llm import handle_query
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "data", "business.db")

class QueryRequest(BaseModel):
    question: str

# ─── Root Endpoint ────────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {"message": "API is running"}

# ─── Chat Endpoint ────────────────────────────────────────────────────────────

@app.post("/query")
def query(req: QueryRequest):
    result = handle_query(req.question)
    return result

# ─── Graph Endpoints ──────────────────────────────────────────────────────────

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@app.get("/graph/overview")
def graph_overview():
    """Returns top level node counts for the graph summary view"""
    conn = get_conn()
    cur = conn.cursor()

    stats = {}

    cur.execute("SELECT COUNT(*) FROM sales_order_headers")
    stats["salesOrders"] = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM delivery_headers")
    stats["deliveries"] = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM billing_headers")
    stats["billingDocuments"] = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM business_partners")
    stats["customers"] = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM products")
    stats["products"] = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM journal_entries")
    stats["journalEntries"] = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM payments")
    stats["payments"] = cur.fetchone()[0]

    conn.close()
    return stats

@app.get("/graph/nodes")
def graph_nodes():
    """Returns nodes and edges for the graph visualization"""
    conn = get_conn()
    cur = conn.cursor()

    nodes = []
    edges = []

    # Sales orders
    cur.execute("SELECT salesOrder, soldToParty, totalNetAmount, overallDeliveryStatus, overallOrdReltdBillgStatus FROM sales_order_headers LIMIT 50")
    for row in cur.fetchall():
        nodes.append({
            "id": f"so_{row['salesOrder']}",
            "type": "salesOrder",
            "label": f"SO {row['salesOrder']}",
            "data": dict(row)
        })

    # Deliveries
    cur.execute("SELECT deliveryDocument, referenceSdDocument FROM delivery_items GROUP BY deliveryDocument")
    for row in cur.fetchall():
        nodes.append({
            "id": f"del_{row['deliveryDocument']}",
            "type": "delivery",
            "label": f"DEL {row['deliveryDocument']}",
            "data": dict(row)
        })
        edges.append({
            "source": f"so_{row['referenceSdDocument']}",
            "target": f"del_{row['deliveryDocument']}",
            "label": "FULFILLED_BY"
        })

    # Billing documents
    cur.execute("SELECT billingDocument, referenceSdDocument FROM billing_items GROUP BY billingDocument")
    for row in cur.fetchall():
        nodes.append({
            "id": f"bil_{row['billingDocument']}",
            "type": "billing",
            "label": f"BIL {row['billingDocument']}",
            "data": dict(row)
        })
        edges.append({
            "source": f"del_{row['referenceSdDocument']}",
            "target": f"bil_{row['billingDocument']}",
            "label": "BILLED_AS"
        })

    # Business partners
    cur.execute("SELECT businessPartner, businessPartnerFullName, industry FROM business_partners")
    for row in cur.fetchall():
        nodes.append({
            "id": f"bp_{row['businessPartner']}",
            "type": "customer",
            "label": row['businessPartnerFullName'] or f"BP {row['businessPartner']}",
            "data": dict(row)
        })

    # Customer to sales order edges
    cur.execute("SELECT salesOrder, soldToParty FROM sales_order_headers")
    for row in cur.fetchall():
        edges.append({
            "source": f"bp_{row['soldToParty']}",
            "target": f"so_{row['salesOrder']}",
            "label": "PLACED"
        })

    # Journal entries
    cur.execute("SELECT accountingDocument, companyCode, postingDate FROM journal_entries GROUP BY accountingDocument LIMIT 50")
    for row in cur.fetchall():
        nodes.append({
            "id": f"je_{row['accountingDocument']}",
            "type": "journalEntry",
            "label": f"JE {row['accountingDocument']}",
            "data": dict(row)
        })

    # Billing to journal edges
    cur.execute("SELECT billingDocument, accountingDocument FROM billing_headers WHERE accountingDocument != ''")
    for row in cur.fetchall():
        edges.append({
            "source": f"bil_{row['billingDocument']}",
            "target": f"je_{row['accountingDocument']}",
            "label": "POSTED_AS"
        })

    conn.close()
    return {"nodes": nodes, "edges": edges}

@app.get("/graph/node/{node_id}")
def get_node_detail(node_id: str):
    """Returns full details for a specific node"""
    conn = get_conn()
    cur = conn.cursor()

    parts = node_id.split("_", 1)
    if len(parts) < 2:
        return {"error": "Invalid node id"}

    prefix, entity_id = parts[0], parts[1]

    if prefix == "so":
        cur.execute("SELECT * FROM sales_order_headers WHERE salesOrder = ?", (entity_id,))
        row = cur.fetchone()
        items_cur = conn.cursor()
        items_cur.execute("SELECT * FROM sales_order_items WHERE salesOrder = ?", (entity_id,))
        items = [dict(r) for r in items_cur.fetchall()]
        conn.close()
        return {"node": dict(row) if row else {}, "items": items}

    elif prefix == "del":
        cur.execute("SELECT * FROM delivery_headers WHERE deliveryDocument = ?", (entity_id,))
        row = cur.fetchone()
        items_cur = conn.cursor()
        items_cur.execute("SELECT * FROM delivery_items WHERE deliveryDocument = ?", (entity_id,))
        items = [dict(r) for r in items_cur.fetchall()]
        conn.close()
        return {"node": dict(row) if row else {}, "items": items}

    elif prefix == "bil":
        cur.execute("SELECT * FROM billing_headers WHERE billingDocument = ?", (entity_id,))
        row = cur.fetchone()
        items_cur = conn.cursor()
        items_cur.execute("SELECT * FROM billing_items WHERE billingDocument = ?", (entity_id,))
        items = [dict(r) for r in items_cur.fetchall()]
        conn.close()
        return {"node": dict(row) if row else {}, "items": items}

    elif prefix == "bp":
        cur.execute("SELECT * FROM business_partners WHERE businessPartner = ?", (entity_id,))
        row = cur.fetchone()
        conn.close()
        return {"node": dict(row) if row else {}}

    elif prefix == "je":
        cur.execute("SELECT * FROM journal_entries WHERE accountingDocument = ?", (entity_id,))
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return {"node": rows}

    conn.close()
    return {"error": "Unknown node type"}

# ─── Run ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)