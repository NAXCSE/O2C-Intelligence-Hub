import sqlite3
import json
import os
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
                "content": "You are a helpful business analyst. Answer the user's question based ONLY on the SQL query results provided. Be concise and specific. Use numbers and IDs from the data. Do not make up any information not present in the results."
            },
            {
                "role": "user",
                "content": f"Question: {user_question}\n\nSQL used: {sql}\n\nResults: {results_str}\n\nProvide a clear, data-backed answer."
            }
        ],
        max_tokens=500,
        temperature=0
    )

    return response.choices[0].message.content.strip()

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