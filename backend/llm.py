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

KEY RELATIONSHIPS:
- delivery_items.referenceSdDocument = sales_order_headers.salesOrder
- billing_items.referenceSdDocument = delivery_items.deliveryDocument
- billing_headers.accountingDocument = journal_entries.accountingDocument
- journal_entries.clearingAccountingDocument = payments.accountingDocument
- sales_order_headers.soldToParty = business_partners.businessPartner
- sales_order_items.material = products.product
- delivery_items.plant = plants.plant
"""

SYSTEM_PROMPT = f"""You are a SQL query generator for an SAP Order-to-Cash (O2C) business dataset.

The O2C flow is: Sales Order -> Delivery -> Billing Document -> Journal Entry -> Payment

You have access to these SQLite tables:
{SCHEMA}

CRITICAL RULES:
1. If the question is related to this dataset, respond with ONLY a valid SQLite SQL query. No explanation, no markdown, no backticks, no comments.
2. If the question is NOT related to this dataset (general knowledge, creative writing, personal questions, anything outside business/orders/deliveries/billing/payments/products/customers), respond with exactly: UNRELATED_QUERY
3. Never use columns that do not exist in the schema above
4. Always use proper SQLite syntax
5. Limit results to 50 rows maximum unless the user asks for all
6. Return raw SQL only — no markdown fences, no explanation, nothing else

QUERY REASONING RULES - THINK BEFORE WRITING SQL:

For FLOW/TRACE questions (trace an order through O2C):
- Start from billing_headers and JOIN outward in both directions using items tables
- Always use: billing_headers → billing_items → delivery_items → sales_order headers
- Never join salesOrder directly to deliveryDocument or billingDocument
- Join through items tables for proper relationships

For "WHICH X HAS MOST/LEAST Y" questions (rankings, comparisons):
- Use COUNT with GROUP BY and ORDER BY
- Always JOIN through items tables, never direct joins
- Include the count in results and order properly

For INCOMPLETE/BROKEN FLOW questions (missing stages):
- Use LEFT JOIN to find missing records
- Check for NULL on the missing step to identify incomplete flows
- Example: LEFT JOIN billing_items ... WHERE billing_items.billingDocument IS NULL

For BLOCKED questions (delivery blockers, billing blockers):
- Check for non-empty block reason columns
- Filter: WHERE headerBillingBlockReason IS NOT NULL AND headerBillingBlockReason != ''

For CUSTOMER questions (customer names, customer metrics):
- Always LEFT JOIN business_partners
- Use businessPartnerFullName for human-readable names
- Join: sales_order_headers.soldToParty = business_partners.businessPartner

For PRODUCT questions (product info, product metrics):
- Always LEFT JOIN product_descriptions
- Filter: ON language = 'EN' for English descriptions
- Use productDescription for human-readable product names

For AMOUNT/REVENUE questions (sum, total, average):
- Always CAST amounts to REAL before aggregating
- Use ROUND(amount, 2) for currency fields
- CAST(ROUND(CAST(totalNetAmount AS REAL), 2) AS TEXT)

For DATE filtering (specific year, date range):
- Use LIKE pattern for year: WHERE billingDocumentDate LIKE '2025%'
- Use BETWEEN for date ranges: WHERE billingDocumentDate BETWEEN '2025-01-01' AND '2025-12-31'

IMPORTANT JOIN RULES:
- delivery_items.referenceSdDocument = sales_order_headers.salesOrder
- billing_items.referenceSdDocument = delivery_items.deliveryDocument
- billing_headers.accountingDocument = journal_entries.accountingDocument
- journal_entries.clearingAccountingDocument = payments.accountingDocument
- sales_order_headers.soldToParty = business_partners.businessPartner
- sales_order_items.material = products.product
"""

def query_to_sql(user_question: str) -> str:
    client = Groq(api_key=os.getenv("GROQ_API_KEY"))

    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_question}
        ],
        max_tokens=500,
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

def detect_query_type(question: str, results: list) -> str:
    """Detect the type of query from the question and result structure."""
    question_lower = question.lower()
    keys = list(results[0].keys()) if results else []
    
    if any(word in question_lower for word in ['trace', 'full flow', 'flow of']):
        return 'trace'
    if any(word in question_lower for word in ['highest', 'most', 'top', 'lowest', 'least', 'ranking']):
        return 'ranking'
    if any(word in question_lower for word in ['incomplete', 'missing', 'never billed', 'not billed', 'no delivery', 'broken']):
        return 'anomaly'
    if any(word in question_lower for word in ['total', 'sum', 'average', 'revenue', 'amount']):
        return 'aggregation'
    if len(results) == 1:
        return 'single_record'
    return 'general'

def sql_results_to_answer(user_question: str, sql: str, results: list) -> str:
    client = Groq(api_key=os.getenv("GROQ_API_KEY"))
    
    query_type = detect_query_type(user_question, results)
    results_str = json.dumps(results[:20], indent=2)

    type_formatting = {
        'trace': """For TRACE queries: Format as a step-by-step flow showing each stage on its own line.
Label each step: Sales Order, Delivery, Billing, Journal Entry, Payment.
Show key fields for each step using format: Stage: ID | Field: Value | Another: Value
End with whether the flow is complete or has gaps.""",
        
        'ranking': """For RANKING queries: Lead with the top result clearly stated in a complete sentence.
List remaining results as comma-separated values with their counts.
End with total count of items found.""",
        
        'anomaly': """For ANOMALY queries: State the count of affected records first.
List the specific IDs or names clearly and completely.
Explain what is missing or broken in the flow.
End with business impact explanation.""",
        
        'aggregation': """For AGGREGATION queries: Show the highest and lowest values found.
Give total and average if relevant to the question.
Put each key number on its own line.
End with one insight.""",
        
        'single_record': """For SINGLE RECORD queries: Show as Label: Value pairs, one per line.
Group related fields by blank lines between groups.
Keep lines under 100 characters.""",
        
        'general': """For GENERAL queries: One clear sentence per insight.
Most important finding first.
Build from specific facts to broader conclusions."""
    }

    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {
                "role": "system",
                "content": f"""You are a business analyst specializing in SAP Order-to-Cash data.
Answer the user's question based ONLY on the SQL query results provided.

QUERY TYPE DETECTED: {query_type.upper()}

{type_formatting.get(query_type, type_formatting['general'])}

UNIVERSAL FORMATTING RULES (FOR ALL QUERY TYPES):
- NEVER use bullet points, asterisks (*), dashes (-), or markdown formatting
- Do NOT use backticks or code formatting
- Write in clean, professional text only
- Use actual values from results (IDs, amounts, dates, names)
- Do not make up information
- Add blank lines between logical sections only when needed
- Always include one final summary or insight line
- Do NOT add comparisons beyond what was asked
- Limit response to maximum 8 lines
- If any amount field shows a negative value, flag it as a reversal or credit
- If billingDocumentIsCancelled is True in results, add at end: "Note: This document is cancelled."
"""
            },
            {
                "role": "user",
                "content": f"Question: {user_question}\n\nSQL used: {sql}\n\nResults: {results_str}\n\nProvide formatted answer:"
            }
        ],
        max_tokens=400,
        temperature=0
    )

    answer = response.choices[0].message.content.strip()
    
    # Post-processing: clean formatting while preserving intentional blank lines
    answer = re.sub(r'\s*\*\s+', ', ', answer)  # Replace "* " with ", "
    answer = re.sub(r',\s*,', ',', answer)       # Remove double commas
    answer = re.sub(r',\s*$', '', answer)        # Remove trailing comma
    answer = re.sub(r'^\s*,', '', answer)        # Remove leading comma
    answer = re.sub(r'\n{4,}', '\n\n', answer)   # Collapse 4+ newlines to 2 (blank line)
    
    return answer

def handle_query(user_question: str) -> dict:
    # Step 1: Convert to SQL
    sql = query_to_sql(user_question)

    # Clean up in case model adds backticks despite instructions
    sql = sql.replace("```sql", "").replace("```", "").strip()

    # Step 2: Check if unrelated
    if "UNRELATED_QUERY" in sql:
        return {
            "answer": "This system is designed to answer questions related to the provided SAP Order-to-Cash dataset only.",
            "sql": None,
            "results": []
        }

    # Step 3: Execute SQL
    try:
        results = execute_sql(sql)
    except Exception as e:
        return {
            "answer": "I understood your question but encountered an error running the query. Please try rephrasing.",
            "sql": sql,
            "results": [],
            "error": str(e)
        }

    # Step 4: Handle empty results
    if not results:
        return {
            "answer": "The query returned no results. No records match your criteria in the dataset.",
            "sql": sql,
            "results": []
        }

    # Step 5: Generate natural language answer
    answer = sql_results_to_answer(user_question, sql, results)

    return {
        "answer": answer,
        "sql": sql,
        "results": results
    }


if __name__ == "__main__":
    test_questions = [
        "Which products are associated with the highest number of billing documents?",
        "Which sales orders were delivered but never billed?",
        "Which customers have the most incomplete order flows?",
        "What is the capital of France?",
    ]

    for q in test_questions:
        print(f"\nQ: {q}")
        print("-" * 60)
        result = handle_query(q)
        print(f"SQL:\n{result.get('sql')}")
        print(f"\nAnswer:\n{result['answer']}")
        print()