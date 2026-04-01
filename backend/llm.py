import sqlite3
import json
import os
import re
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "data", "business.db")

SCHEMA = """
Tables and columns:

sales_order_headers: salesOrder, salesOrderType, salesOrganization, distributionChannel, organizationDivision, salesGroup, salesOffice, soldToParty, creationDate, createdByUser, lastChangeDateTime, totalNetAmount, overallDeliveryStatus, overallOrdReltdBillgStatus, overallSdDocReferenceStatus, transactionCurrency, pricingDate, requestedDeliveryDate, headerBillingBlockReason, deliveryBlockReason, incotermsClassification, incotermsLocation1, customerPaymentTerms, totalCreditCheckStatus

sales_order_items: salesOrder, salesOrderItem, salesOrderItemCategory, material, requestedQuantity, requestedQuantityUnit, transactionCurrency, netAmount, materialGroup, productionPlant, storageLocation, salesDocumentRjcnReason, itemBillingBlockReason

delivery_headers: actualGoodsMovementDate, actualGoodsMovementTime, creationDate, creationTime, deliveryBlockReason, deliveryDocument, hdrGeneralIncompletionStatus, headerBillingBlockReason, lastChangeDate, overallGoodsMovementStatus, overallPickingStatus, overallProofOfDeliveryStatus, shippingPoint

delivery_items: actualDeliveryQuantity, batch, deliveryDocument, deliveryDocumentItem, deliveryQuantityUnit, itemBillingBlockReason, lastChangeDate, plant, referenceSdDocument, referenceSdDocumentItem, storageLocation

billing_headers: billingDocument, billingDocumentType, creationDate, creationTime, lastChangeDateTime, billingDocumentDate, billingDocumentIsCancelled, cancelledBillingDocument, totalNetAmount, transactionCurrency, companyCode, fiscalYear, accountingDocument, soldToParty

billing_items: billingDocument, billingDocumentItem, material, billingQuantity, billingQuantityUnit, netAmount, transactionCurrency, referenceSdDocument, referenceSdDocumentItem

billing_cancellations: billingDocument, billingDocumentType, creationDate, billingDocumentIsCancelled, cancelledBillingDocument, totalNetAmount, transactionCurrency, companyCode, fiscalYear, accountingDocument, soldToParty

journal_entries: companyCode, fiscalYear, accountingDocument, glAccount, referenceDocument, costCenter, profitCenter, transactionCurrency, amountInTransactionCurrency, companyCodeCurrency, amountInCompanyCodeCurrency, postingDate, documentDate, accountingDocumentType, accountingDocumentItem, assignmentReference, lastChangeDateTime, customer, financialAccountType, clearingDate, clearingAccountingDocument, clearingDocFiscalYear

payments: companyCode, fiscalYear, accountingDocument, accountingDocumentItem, clearingDate, clearingAccountingDocument, clearingDocFiscalYear, amountInTransactionCurrency, transactionCurrency, amountInCompanyCodeCurrency, companyCodeCurrency, customer, invoiceReference, invoiceReferenceFiscalYear, salesDocument, salesDocumentItem, postingDate, documentDate, assignmentReference, glAccount, financialAccountType, profitCenter, costCenter

business_partners: businessPartner, customer, businessPartnerCategory, businessPartnerFullName, businessPartnerGrouping, businessPartnerName, correspondenceLanguage, createdByUser, creationDate, creationTime, firstName, formOfAddress, industry, lastChangeDate, lastName, organizationBpName1, organizationBpName2, businessPartnerIsBlocked, isMarkedForArchiving

bp_addresses: businessPartner, addressId, validityStartDate, validityEndDate, cityName, country, postalCode, region, streetName

products: product, productType, crossPlantStatus, creationDate, createdByUser, lastChangeDate, isMarkedForDeletion, productOldId, grossWeight, weightUnit, netWeight, productGroup, baseUnit, division, industrySector

product_descriptions: product, language, productDescription

plants: plant, plantName, valuationArea, salesOrganization, addressId, plantCategory, distributionChannel, division, language
"""

SYSTEM_PROMPT = f"""You are an expert SQLite query generator for an SAP Order-to-Cash (O2C) business dataset.

The O2C flow is always in this direction:
Sales Order → Delivery → Billing Document → Journal Entry → Payment

{SCHEMA}

═══════════════════════════════════════════════════
RULE 1 — ONLY VALID JOINS (MOST IMPORTANT RULE)
═══════════════════════════════════════════════════

These are the ONLY valid foreign key relationships. Never invent others.

delivery_items.referenceSdDocument    = sales_order_headers.salesOrder
delivery_items.deliveryDocument       = delivery_headers.deliveryDocument
billing_items.referenceSdDocument     = delivery_items.deliveryDocument
billing_items.billingDocument         = billing_headers.billingDocument
billing_headers.accountingDocument    = journal_entries.accountingDocument
journal_entries.clearingAccountingDocument = payments.accountingDocument
sales_order_headers.soldToParty       = business_partners.businessPartner
sales_order_items.material            = products.product
delivery_items.plant                  = plants.plant

═══════════════════════════════════════════════════
RULE 2 — HEADER vs ITEMS TABLES
═══════════════════════════════════════════════════

delivery_headers has NO foreign key to sales_order_headers.
To check if a sales order has a delivery, use delivery_items:
  CORRECT: LEFT JOIN delivery_items di ON di.referenceSdDocument = soh.salesOrder
  WRONG:   LEFT JOIN delivery_headers dh ON dh.deliveryDocument = soh.salesOrder

billing_headers has NO foreign key to delivery_items.
To check if a delivery has billing, use billing_items:
  CORRECT: LEFT JOIN billing_items bi ON bi.referenceSdDocument = di.deliveryDocument
  WRONG:   LEFT JOIN billing_headers bh ON bh.billingDocument = di.deliveryDocument

Only join delivery_headers when you need its specific columns.
In that case join it through delivery_items:
  JOIN delivery_headers dh ON dh.deliveryDocument = di.deliveryDocument

Only join billing_headers when you need its specific columns.
In that case join it through billing_items:
  JOIN billing_headers bh ON bh.billingDocument = bi.billingDocument

═══════════════════════════════════════════════════
RULE 3 — NEVER USE ITEM-LEVEL JOIN CONDITIONS
═══════════════════════════════════════════════════

When joining O2C tables only match on the document number.
Never add secondary item number conditions.

  CORRECT: JOIN delivery_items di ON di.referenceSdDocument = soh.salesOrder
  WRONG:   JOIN delivery_items di ON di.referenceSdDocument = soh.salesOrder
                                 AND di.referenceSdDocumentItem = soi.salesOrderItem

  CORRECT: JOIN billing_items bi ON bi.referenceSdDocument = di.deliveryDocument
  WRONG:   JOIN billing_items bi ON bi.referenceSdDocument = di.deliveryDocument
                                AND bi.referenceSdDocumentItem = di.deliveryDocumentItem

═══════════════════════════════════════════════════
RULE 4 — QUERY PATTERNS BY TYPE
═══════════════════════════════════════════════════

TRACE/FLOW queries (trace billing document X):
  FROM billing_headers bh
  JOIN billing_items bi ON bi.billingDocument = bh.billingDocument
  JOIN delivery_items di ON di.deliveryDocument = bi.referenceSdDocument
  JOIN sales_order_headers soh ON soh.salesOrder = di.referenceSdDocument
  LEFT JOIN journal_entries je ON je.accountingDocument = bh.accountingDocument
  LEFT JOIN payments p ON p.accountingDocument = je.clearingAccountingDocument
  WHERE bh.billingDocument = 'X'

DELIVERED BUT NOT BILLED (missing billing):
  FROM sales_order_headers soh
  JOIN delivery_items di ON di.referenceSdDocument = soh.salesOrder
  LEFT JOIN billing_items bi ON bi.referenceSdDocument = di.deliveryDocument
  WHERE bi.billingDocument IS NULL

NOT DELIVERED (missing delivery):
  FROM sales_order_headers soh
  LEFT JOIN delivery_items di ON di.referenceSdDocument = soh.salesOrder
  WHERE di.deliveryDocument IS NULL

INCOMPLETE FLOW (missing any step):
  FROM sales_order_headers soh
  LEFT JOIN business_partners bp ON bp.businessPartner = soh.soldToParty
  LEFT JOIN delivery_items di ON di.referenceSdDocument = soh.salesOrder
  LEFT JOIN billing_items bi ON bi.referenceSdDocument = di.deliveryDocument
  WHERE di.deliveryDocument IS NULL OR bi.billingDocument IS NULL
  GROUP BY soh.soldToParty

PRODUCT BILLING COUNT (which products appear most in billing):
  FROM sales_order_items soi
  JOIN delivery_items di ON di.referenceSdDocument = soi.salesOrder
  JOIN billing_items bi ON bi.referenceSdDocument = di.deliveryDocument
  GROUP BY soi.material
  ORDER BY COUNT(DISTINCT bi.billingDocument) DESC

CUSTOMER QUESTIONS: always JOIN business_partners and show businessPartnerFullName
PRODUCT QUESTIONS: always LEFT JOIN product_descriptions ON language = 'EN'
AMOUNT QUESTIONS: always CAST(amount AS REAL), use ROUND(value, 2)
DATE FILTERING: use LIKE '2025%' for year, BETWEEN for ranges

═══════════════════════════════════════════════════
RULE 5 — INCOMPLETE FLOW RULE
═══════════════════════════════════════════════════

INCOMPLETE FLOW RULE (critical fix):
For incomplete order flow questions, check ALL stages including
missing journal entries and missing payments, not just missing delivery.

CORRECT pattern - checks every stage of O2C:
SELECT 
    bp.businessPartnerFullName,
    bp.businessPartner,
    COUNT(DISTINCT CASE WHEN di.deliveryDocument IS NULL THEN soh.salesOrder END) as missing_delivery,
    COUNT(DISTINCT CASE WHEN bi.billingDocument IS NULL THEN soh.salesOrder END) as missing_billing,
    COUNT(DISTINCT CASE WHEN je.accountingDocument IS NULL THEN soh.salesOrder END) as missing_journal,
    COUNT(DISTINCT CASE WHEN p.accountingDocument IS NULL THEN soh.salesOrder END) as missing_payment,
    COUNT(DISTINCT soh.salesOrder) as total_incomplete
FROM sales_order_headers soh
LEFT JOIN business_partners bp ON bp.businessPartner = soh.soldToParty
LEFT JOIN delivery_items di ON di.referenceSdDocument = soh.salesOrder
LEFT JOIN billing_items bi ON bi.referenceSdDocument = di.deliveryDocument
LEFT JOIN billing_headers bh ON bh.billingDocument = bi.billingDocument
LEFT JOIN journal_entries je ON je.accountingDocument = bh.accountingDocument
LEFT JOIN payments p ON p.accountingDocument = je.clearingAccountingDocument
WHERE di.deliveryDocument IS NULL 
   OR bi.billingDocument IS NULL 
   OR je.accountingDocument IS NULL 
   OR p.accountingDocument IS NULL
GROUP BY soh.soldToParty, bp.businessPartnerFullName
ORDER BY total_incomplete DESC

CRITICAL: Always GROUP BY soh.soldToParty not bp.businessPartnerFullName alone.
Always include all four NULL checks in the WHERE clause.


═══════════════════════════════════════════════════
RULE 6 — OUTPUT FORMAT
═══════════════════════════════════════════════════

1. Return ONLY raw SQLite SQL. No markdown, no backticks, no explanation.
2. For off-topic questions return exactly: UNRELATED_QUERY
3. Never use columns not in the schema.
4. Limit to 50 rows unless user asks for all.
"""


def query_to_sql(user_question: str) -> str:
    client = Groq(api_key=os.getenv("GROQ_API_KEY"))
    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_question}
        ],
        max_tokens=600,
        temperature=0
    )
    return response.choices[0].message.content.strip()


def execute_sql(sql: str) -> list:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def is_health_check_query(question: str):
    question_lower = question.lower()
    health_keywords = [
        'problem', 'issue', 'wrong', 'stuck', 'blocked',
        'what happened', 'status of', 'anything wrong',
        'errors', 'failed', 'related to sales order'
    ]
    has_keyword = any(word in question_lower for word in health_keywords)
    match = re.search(r'\b(7\d{5})\b', question)
    if has_keyword and match:
        return match.group(1)
    return None


def execute_health_check(order_id: str):
    sql = """
    SELECT
        soh.salesOrder,
        soh.soldToParty,
        bp.businessPartnerFullName,
        soh.totalNetAmount,
        soh.transactionCurrency,
        soh.overallDeliveryStatus,
        soh.overallOrdReltdBillgStatus,
        soh.headerBillingBlockReason,
        soh.deliveryBlockReason,
        soi.itemBillingBlockReason,
        soi.salesDocumentRjcnReason,
        di.deliveryDocument,
        di.plant,
        bi.billingDocument,
        bh.billingDocumentIsCancelled,
        je.accountingDocument,
        je.postingDate,
        p.accountingDocument AS paymentDoc,
        p.clearingDate AS paymentDate,
        p.amountInTransactionCurrency AS paymentAmount
    FROM sales_order_headers soh
    LEFT JOIN business_partners bp ON bp.businessPartner = soh.soldToParty
    LEFT JOIN sales_order_items soi ON soi.salesOrder = soh.salesOrder
    LEFT JOIN delivery_items di ON di.referenceSdDocument = soh.salesOrder
    LEFT JOIN billing_items bi ON bi.referenceSdDocument = di.deliveryDocument
    LEFT JOIN billing_headers bh ON bh.billingDocument = bi.billingDocument
    LEFT JOIN journal_entries je ON je.accountingDocument = bh.accountingDocument
    LEFT JOIN payments p ON p.accountingDocument = je.clearingAccountingDocument
    WHERE soh.salesOrder = ?
    LIMIT 1
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(sql, (order_id,))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows, sql


def format_health_check_answer(order_id: str, results: list) -> str:
    if not results:
        return f"Sales order {order_id} was not found in the dataset."

    r = results[0]
    problems = []
    lines = []

    customer = r.get('businessPartnerFullName') or r.get('soldToParty', 'Unknown')
    amount = r.get('totalNetAmount', 'N/A')
    currency = r.get('transactionCurrency', '')
    lines.append(f"Health check for Sales Order {order_id} | Customer: {customer} | Amount: {amount} {currency}")
    lines.append("")

    # Delivery
    if not r.get('deliveryDocument'):
        lines.append("Delivery: MISSING — no delivery document found.")
        problems.append("missing delivery")
    else:
        block = r.get('deliveryBlockReason', '')
        if block and block.strip():
            lines.append(f"Delivery: BLOCKED — reason code: {block}")
            problems.append("delivery blocked")
        else:
            lines.append(f"Delivery: OK — document {r['deliveryDocument']} at plant {r.get('plant', 'N/A')}")

    # Billing
    if not r.get('billingDocument'):
        lines.append("Billing: MISSING — no billing document found.")
        problems.append("missing billing")
    else:
        cancelled = r.get('billingDocumentIsCancelled', '')
        billing_block = r.get('headerBillingBlockReason', '') or r.get('itemBillingBlockReason', '')
        if str(cancelled).lower() == 'true':
            lines.append(f"Billing: CANCELLED — document {r['billingDocument']} has been cancelled.")
            problems.append("billing cancelled")
        elif billing_block and billing_block.strip():
            lines.append(f"Billing: BLOCKED — reason code: {billing_block}")
            problems.append("billing blocked")
        else:
            lines.append(f"Billing: OK — document {r['billingDocument']}")

    # Rejection
    rejection = r.get('salesDocumentRjcnReason', '')
    if rejection and rejection.strip():
        lines.append(f"Rejection: FOUND — reason code: {rejection}")
        problems.append("order rejected")

    # Journal Entry
    if not r.get('accountingDocument'):
        lines.append("Journal Entry: MISSING — no accounting document posted.")
        problems.append("missing journal entry")
    else:
        lines.append(f"Journal Entry: OK — document {r['accountingDocument']} posted on {r.get('postingDate', 'N/A')}")

    # Payment
    if not r.get('paymentDoc'):
        lines.append("Payment: PENDING — no payment recorded yet.")
        problems.append("payment pending")
    else:
        lines.append(f"Payment: OK — cleared on {r.get('paymentDate', 'N/A')} | Amount: {r.get('paymentAmount', '')} {currency}")

    lines.append("")
    if problems:
        lines.append(f"{len(problems)} problem(s) found: {', '.join(problems)}.")
    else:
        lines.append("No problems found. This order has a complete O2C flow.")

    return "\n".join(lines)


def detect_query_type(question: str, results: list) -> str:
    question_lower = question.lower()
    if any(w in question_lower for w in ['trace', 'full flow', 'flow of']):
        return 'trace'
    if any(w in question_lower for w in ['highest', 'most', 'top', 'lowest', 'least', 'ranking']):
        return 'ranking'
    if any(w in question_lower for w in ['incomplete', 'missing', 'never billed', 'not billed', 'no delivery', 'broken', 'delivered but']):
        return 'anomaly'
    if any(w in question_lower for w in ['total', 'sum', 'average', 'revenue', 'amount']):
        return 'aggregation'
    if len(results) == 1:
        return 'single_record'
    return 'general'


def sql_results_to_answer(user_question: str, sql: str, results: list) -> str:
    client = Groq(api_key=os.getenv("GROQ_API_KEY"))
    query_type = detect_query_type(user_question, results)
    results_str = json.dumps(results[:20], indent=2)

    type_formatting = {
        'trace': """Format as a step-by-step flow. Each stage on its own line.
Use format: Stage: ID | Field: Value | Field: Value
Stages in order: Sales Order, Delivery, Billing, Journal Entry, Payment.
End with: flow is complete or state what is missing.""",

        'ranking': """Lead with the top result in one sentence.
Second line: list remaining items as comma-separated values with counts.
Final line: total number of items found.""",

        'anomaly': """First line: count of affected records.
Second line: list all IDs or names.
Third line: explain what is missing or broken.
Final line: business impact in one sentence.""",

        'aggregation': """First line: highest value found.
Second line: lowest value found.
Third line: total or average if relevant.
Final line: one business insight.""",

        'single_record': """Show as Label: Value pairs one per line.
Group related fields together.
Keep each line under 100 characters.""",

        'general': """One sentence per insight.
Most important finding first.
Maximum 5 lines."""
    }

    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {
                "role": "system",
                "content": f"""You are a business analyst for SAP Order-to-Cash data.
Answer ONLY based on the SQL results provided. Never invent data.

QUERY TYPE: {query_type.upper()}

{type_formatting.get(query_type, type_formatting['general'])}

FORMATTING RULES FOR ALL TYPES:
- Never use bullet points, asterisks, dashes, or markdown
- Each sentence on its own line
- Negative amounts = flag as reversal or credit
- Only add "Note: This document is cancelled." if billingDocumentIsCancelled 
  field exists in results AND its value is literally the string 'True'.
  Never add this note if the field is absent or has any other value.
- Maximum 8 lines total"""
            },
            {
                "role": "user",
                "content": f"Question: {user_question}\n\nSQL: {sql}\n\nResults: {results_str}\n\nAnswer:"
            }
        ],
        max_tokens=400,
        temperature=0
    )

    answer = response.choices[0].message.content.strip()
    answer = re.sub(r'\s*\*\s+', ' ', answer)
    answer = re.sub(r'\n{4,}', '\n\n', answer)
    sentences = re.split(r'(?<=\.)\s+', answer)
    return '\n'.join(s.strip() for s in sentences if s.strip())


def handle_query(user_question: str) -> dict:

    # Health check intercept — bypasses LLM for reliability
    order_id = is_health_check_query(user_question)
    if order_id:
        results, sql = execute_health_check(order_id)
        answer = format_health_check_answer(order_id, results)
        return {"answer": answer, "sql": sql, "results": results}

    # Step 1: NL to SQL
    sql = query_to_sql(user_question)
    sql = sql.replace("```sql", "").replace("```", "").strip()

    # Step 2: Guardrail
    if "UNRELATED_QUERY" in sql:
        return {
            "answer": "This system is designed to answer questions related to the provided SAP Order-to-Cash dataset only.",
            "sql": None,
            "results": []
        }

    # Step 3: Execute
    try:
        results = execute_sql(sql)
    except Exception as e:
        return {
            "answer": "I understood your question but encountered an error running the query. Please try rephrasing.",
            "sql": sql,
            "results": [],
            "error": str(e)
        }

    # Step 4: Empty results
    if not results:
        return {
            "answer": "The query returned no results. No records match your criteria in the dataset.",
            "sql": sql,
            "results": []
        }

    # Step 5: Generate answer
    answer = sql_results_to_answer(user_question, sql, results)
    return {"answer": answer, "sql": sql, "results": results}


if __name__ == "__main__":
    test_questions = [
        "Which products are associated with the highest number of billing documents?",
        "Which sales orders were delivered but never billed?",
        "Which customers have the most incomplete order flows?",
        "Trace the full flow of billing document 90504204",
        "What are the problems with sales order 740537?",
        "What is the capital of France?",
    ]

    for q in test_questions:
        print(f"\nQ: {q}")
        print("-" * 60)
        result = handle_query(q)
        print(f"SQL:\n{result.get('sql')}")
        print(f"\nAnswer:\n{result['answer']}")
        print()