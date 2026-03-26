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

Rules:
1. If the question is related to this dataset, respond with ONLY a valid SQLite SQL query. No explanation, no markdown, no backticks, no comments.
2. If the question is NOT related to this dataset (general knowledge, creative writing, personal questions, anything outside business/orders/deliveries/billing/payments/products/customers), respond with exactly: UNRELATED_QUERY
3. Never use columns that do not exist in the schema above
4. Always use proper SQLite syntax
5. For text comparisons use LIKE or exact match
6. Limit results to 50 rows maximum unless the user asks for all
7. Return raw SQL only — no markdown fences, no explanation, nothing else

IMPORTANT JOIN RULES:
- To find delivered orders: JOIN sales_order_headers to delivery_items ON delivery_items.referenceSdDocument = sales_order_headers.salesOrder
- To find billed orders: JOIN delivery_items to billing_items ON billing_items.referenceSdDocument = delivery_items.deliveryDocument
- NEVER join salesOrder directly to deliveryDocument or billingDocument
- Always go through the items tables for order-to-delivery-to-billing joins
- For tracing flows: billing_headers → billing_items → delivery_items → sales_order_items/headers

EXAMPLE - Trace the full flow of billing document 90504204:
SELECT bh.billingDocument, bh.billingDocumentDate, bh.totalNetAmount, bh.soldToParty, bi.material, bi.billingQuantity, di.deliveryDocument, di.actualDeliveryQuantity, soh.salesOrder, soh.totalNetAmount AS orderAmount, bp.businessPartnerFullName, je.accountingDocument, je.amountInTransactionCurrency FROM billing_headers bh JOIN billing_items bi ON bh.billingDocument = bi.billingDocument JOIN delivery_items di ON bi.referenceSdDocument = di.deliveryDocument LEFT JOIN sales_order_headers soh ON di.referenceSdDocument = soh.salesOrder LEFT JOIN business_partners bp ON soh.soldToParty = bp.businessPartner LEFT JOIN journal_entries je ON bh.accountingDocument = je.accountingDocument WHERE bh.billingDocument = '90504204'
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

def sql_results_to_answer(user_question: str, sql: str, results: list) -> str:
    client = Groq(api_key=os.getenv("GROQ_API_KEY"))

    results_str = json.dumps(results[:20], indent=2)

    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {
                "role": "system",
                "content": """You are a helpful business analyst. Answer the user's question based ONLY on the SQL query results provided.

CRITICAL RULES:
1. Answer ONLY what was asked - do not add extra comparisons or analysis
2. Do NOT compare entities that were not directly asked about
3. Avoid repetition - say each fact once, clearly
4. Stop after answering the question - no additional rankings or comparisons

FORMATTING RULES (MANDATORY):
- Do NOT use bullet points (*), dashes (-), or markdown formatting
- For lists of IDs or values: use comma-separated format (e.g., "740506, 740507, 740508")
- Add a blank line between different logical groups or topics
- Keep answers concise and specific
- Use numbers and IDs from the data directly
- Write clean, professional text without symbols or special formatting
- Do not make up any information not present in the results

GOOD EXAMPLE (answer is complete, no extra comparisons):
Q: Which customers have the most incomplete order flows?
A: The customers with the most incomplete order flows are Henderson, Garner and Graves and Melton Group, both with 7 incomplete orders.

BAD EXAMPLE (adds unnecessary comparisons):
The customers with the most incomplete order flows are Henderson, Garner and Graves, Melton Group, both with 7 incomplete orders.
Bradley-Kelley has 2 incomplete orders, which is less than Henderson, Garner and Graves...
(This is wrong - don't compare others unless asked)"""
            },
            {
                "role": "user",
                "content": f"Question: {user_question}\n\nSQL used: {sql}\n\nResults: {results_str}\n\nAnswer ONLY the question asked. Do not add extra comparisons. Keep it concise."
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