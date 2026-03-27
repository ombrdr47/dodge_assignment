import json
import os
import glob
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

# ── Connection ────────────────────────────────────────────────────────────────
URI      = os.environ.get("NEO4J_URI",      "bolt://localhost:7687")
USER     = os.environ.get("NEO4J_USER",     "neo4j")
PASSWORD = os.environ.get("NEO4J_PASSWORD", "password")

# ── Constraints (unique index per node type) ──────────────────────────────────
CONSTRAINT_QUERIES = [
    "CREATE CONSTRAINT customer_id        IF NOT EXISTS FOR (n:Customer)        REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT product_id         IF NOT EXISTS FOR (n:Product)         REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT order_id           IF NOT EXISTS FOR (n:SalesOrder)      REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT delivery_id        IF NOT EXISTS FOR (n:Delivery)        REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT billing_id         IF NOT EXISTS FOR (n:BillingDocument) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT journal_id         IF NOT EXISTS FOR (n:JournalEntry)    REQUIRE n.id IS UNIQUE",
]


def setup_constraints(driver):
    print("Setting up Neo4j constraints…")
    with driver.session() as session:
        for q in CONSTRAINT_QUERIES:
            session.run(q)
    print("Constraints ready.\n")


# ── Batch reader ──────────────────────────────────────────────────────────────
def read_jsonl_in_batches(filepath, batch_size=2000):
    """Yields batches of parsed JSON objects from a .jsonl file."""
    batch = []
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                batch.append(json.loads(line))
            except json.JSONDecodeError:
                continue
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch


# ── Node ingestors ────────────────────────────────────────────────────────────

def ingest_customers(driver, filepath):
    """business_partners → Customer nodes."""
    cypher = """
    UNWIND $batch AS row
    MERGE (c:Customer {id: row.businessPartner})
    SET c.name     = row.businessPartnerFullName,
        c.category = row.businessPartnerCategory,
        c.industry  = row.industry
    """
    _run_batches(driver, cypher, filepath)


def ingest_products(driver, filepath):
    """products → Product nodes."""
    cypher = """
    UNWIND $batch AS row
    MERGE (p:Product {id: row.product})
    SET p.type   = row.productType,
        p.group  = row.productGroup,
        p.weight = toFloat(coalesce(row.netWeight, 0))
    """
    _run_batches(driver, cypher, filepath)


def ingest_sales_orders(driver, filepath):
    """sales_order_headers → SalesOrder nodes + Customer-[:PLACED]->SalesOrder."""
    cypher = """
    UNWIND $batch AS row
    MERGE (o:SalesOrder {id: row.salesOrder})
    SET o.type      = row.salesOrderType,
        o.netAmount = toFloat(coalesce(row.totalNetAmount, 0)),
        o.currency  = row.transactionCurrency,
        o.date      = row.creationDate,
        o.status    = row.overallDeliveryStatus

    WITH o, row
    WHERE row.soldToParty IS NOT NULL AND row.soldToParty <> ""
    MERGE (c:Customer {id: row.soldToParty})
    MERGE (c)-[:PLACED]->(o)
    """
    _run_batches(driver, cypher, filepath)


def ingest_sales_order_items(driver, filepath):
    """sales_order_items → SalesOrder-[:CONTAINS]->Product."""
    cypher = """
    UNWIND $batch AS row
    WITH row WHERE row.salesOrder IS NOT NULL
                AND row.material IS NOT NULL
                AND row.material <> ""
    MERGE (o:SalesOrder {id: row.salesOrder})
    MERGE (p:Product    {id: row.material})
    MERGE (o)-[:CONTAINS]->(p)
    """
    _run_batches(driver, cypher, filepath)


def ingest_deliveries(driver, filepath):
    """
    delivery_headers → Delivery nodes
    + SalesOrder-[:HAS_DELIVERY]->Delivery
    + Customer-[:RECEIVED]->Delivery   (optional, if soldToParty present)

    Adjust field names below if your actual JSONL uses different keys.
    Common field names in SAP delivery exports:
      deliveryDocument, actualGoodsMovementDate, overallGoodsMovementStatus,
      salesOrder, soldToParty
    """
    cypher = """
    UNWIND $batch AS row
    MERGE (d:Delivery {id: row.deliveryDocument})
    SET d.date   = coalesce(row.actualGoodsMovementDate, row.plannedGoodsMovementDate),
        d.status = coalesce(row.overallGoodsMovementStatus, row.deliveryStatus)

    // Link to originating SalesOrder
    WITH d, row
    WHERE row.salesOrder IS NOT NULL AND row.salesOrder <> ""
    MERGE (o:SalesOrder {id: row.salesOrder})
    MERGE (o)-[:HAS_DELIVERY]->(d)

    // Link to Customer (if present)
    WITH d, row
    WHERE row.soldToParty IS NOT NULL AND row.soldToParty <> ""
    MERGE (c:Customer {id: row.soldToParty})
    MERGE (c)-[:RECEIVED]->(d)
    """
    _run_batches(driver, cypher, filepath)


def ingest_billing_documents(driver, filepath):
    """billing_document_headers → BillingDocument nodes + BillingDocument-[:BILLS]->Customer."""
    cypher = """
    UNWIND $batch AS row
    MERGE (b:BillingDocument {id: row.billingDocument})
    SET b.type      = row.billingDocumentType,
        b.netAmount = toFloat(coalesce(row.totalNetAmount, 0)),
        b.currency  = row.transactionCurrency,
        b.date      = row.billingDocumentDate

    WITH b, row
    WHERE row.soldToParty IS NOT NULL AND row.soldToParty <> ""
    MERGE (c:Customer {id: row.soldToParty})
    MERGE (b)-[:BILLS]->(c)
    """
    _run_batches(driver, cypher, filepath)


def ingest_billing_document_items(driver, filepath):
    """billing_document_items → BillingDocument-[:REFERENCES]->SalesOrder."""
    cypher = """
    UNWIND $batch AS row
    WITH row WHERE row.billingDocument IS NOT NULL
                AND row.referenceSdDocument IS NOT NULL
                AND row.referenceSdDocument <> ""
    MERGE (b:BillingDocument {id: row.billingDocument})
    MERGE (o:SalesOrder      {id: row.referenceSdDocument})
    MERGE (b)-[:REFERENCES]->(o)
    """
    _run_batches(driver, cypher, filepath)


def ingest_journal_entries(driver, filepath):
    """
    journal_entries → JournalEntry nodes
    + BillingDocument-[:HAS_JOURNAL_ENTRY]->JournalEntry

    Adjust field names to match your actual JSONL schema.
    Common SAP accounting document fields:
      accountingDocument, fiscalYear, glAccount, referenceDocument,
      amountInTransactionCurrency, transactionCurrency,
      amountInCompanyCodeCurrency, companyCodeCurrency,
      postingDate, documentDate, accountingDocumentType,
      accountingDocumentItem, costCenter, profitCenter
    """
    cypher = """
    UNWIND $batch AS row
    // Use a composite key so individual line items don't collide
    MERGE (j:JournalEntry {id: row.accountingDocument + '-' + coalesce(row.accountingDocumentItem, '1')})
    SET j.accountingDocument            = row.accountingDocument,
        j.fiscalYear                    = row.fiscalYear,
        j.glAccount                     = row.glAccount,
        j.amount                        = toFloat(coalesce(row.amountInTransactionCurrency, 0)),
        j.currency                      = row.transactionCurrency,
        j.amountInCompanyCodeCurrency   = toFloat(coalesce(row.amountInCompanyCodeCurrency, 0)),
        j.companyCodeCurrency           = row.companyCodeCurrency,
        j.postingDate                   = row.postingDate,
        j.documentDate                  = row.documentDate,
        j.documentType                  = row.accountingDocumentType,
        j.documentItem                  = row.accountingDocumentItem,
        j.referenceDocument             = row.referenceDocument,
        j.costCenter                    = row.costCenter,
        j.profitCenter                  = row.profitCenter

    // Link to the BillingDocument this entry was generated from
    WITH j, row
    WHERE row.referenceDocument IS NOT NULL AND row.referenceDocument <> ""
    MERGE (b:BillingDocument {id: row.referenceDocument})
    MERGE (b)-[:HAS_JOURNAL_ENTRY]->(j)
    """
    _run_batches(driver, cypher, filepath)


# ── Helper ────────────────────────────────────────────────────────────────────

def _run_batches(driver, cypher: str, filepath: str):
    """Execute a parameterised Cypher query in batches over a JSONL file."""
    with driver.session() as session:
        for batch in read_jsonl_in_batches(filepath):
            session.run(cypher, batch=batch)


def process_directory(driver, base_dir: str, folder_name: str, ingest_func):
    """Run an ingestor over every .jsonl file found in base_dir/folder_name/."""
    pattern = os.path.join(base_dir, folder_name, "*.jsonl")
    files   = glob.glob(pattern)
    if not files:
        print(f"  [SKIP] No .jsonl files found at: {pattern}")
        return
    print(f"  [{folder_name}] {len(files)} file(s) found — ingesting…")
    for f in files:
        ingest_func(driver, f)
    print(f"  [{folder_name}] done.\n")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

    # Dataset root — expects sibling folder "sap-o2c-data" next to this script
    # base_data_dir = os.path.abspath(
    #     os.path.join(os.path.dirname(__file__), "..", "sap-o2c-data")
    # )

    base_data_dir = "/app/sap-o2c-data"

    print(f"Dataset root: {base_data_dir}\n")

    try:
        setup_constraints(driver)

        print("=== Phase 1: Master Data (Nodes) ===")
        process_directory(driver, base_data_dir, "business_partners", ingest_customers)
        process_directory(driver, base_data_dir, "products",          ingest_products)

        print("=== Phase 2: Transactional Nodes + Initial Relationships ===")
        process_directory(driver, base_data_dir, "sales_order_headers",      ingest_sales_orders)
        process_directory(driver, base_data_dir, "outbound_delivery_headers",         ingest_deliveries)
        process_directory(driver, base_data_dir, "billing_document_headers", ingest_billing_documents)
        process_directory(driver, base_data_dir, "journal_entry_items_accounts_receivable",          ingest_journal_entries)

        print("=== Phase 3: Item-Level Relationships ===")
        process_directory(driver, base_data_dir, "sales_order_items",      ingest_sales_order_items)
        process_directory(driver, base_data_dir, "billing_document_items", ingest_billing_document_items)

        print("✅  Data ingestion complete!")

    finally:
        driver.close()
