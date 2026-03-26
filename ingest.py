"""
ingest.py
─────────
Auto-discovers every folder under sap-o2c-data/, sniffs the first record of
each JSONL file to learn actual field names, then maps folders → ingestors
via a flexible keyword match rather than hardcoded strings.

Run:
    python ingest.py                         # uses default ../sap-o2c-data
    python ingest.py /path/to/sap-o2c-data  # custom path
"""

import json
import os
import sys
import glob
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

# ── Connection ────────────────────────────────────────────────────────────────
URI      = os.environ.get("NEO4J_URI",      "bolt://localhost:7687")
USER     = os.environ.get("NEO4J_USER",     "neo4j")
PASSWORD = os.environ.get("NEO4J_PASSWORD", "password")

# ── Constraints ───────────────────────────────────────────────────────────────
CONSTRAINT_QUERIES = [
    "CREATE CONSTRAINT customer_id  IF NOT EXISTS FOR (n:Customer)        REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT product_id   IF NOT EXISTS FOR (n:Product)         REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT order_id     IF NOT EXISTS FOR (n:SalesOrder)      REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT delivery_id  IF NOT EXISTS FOR (n:Delivery)        REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT billing_id   IF NOT EXISTS FOR (n:BillingDocument) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT journal_id   IF NOT EXISTS FOR (n:JournalEntry)    REQUIRE n.id IS UNIQUE",
]

def setup_constraints(driver):
    print("Setting up Neo4j constraints…")
    with driver.session() as session:
        for q in CONSTRAINT_QUERIES:
            session.run(q)
    print("Constraints ready.\n")


# ── JSONL helpers ─────────────────────────────────────────────────────────────

def read_jsonl_in_batches(filepath, batch_size=2000):
    """Yield batches of parsed JSON objects from a .jsonl file."""
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


def sniff_fields(filepath):
    """Return the keys present in the FIRST record of a JSONL file."""
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                return set(json.loads(line).keys())
            except json.JSONDecodeError:
                continue
    return set()


def first_jsonl_in(folder_path):
    """Return the path to the first .jsonl file found in a folder, or None."""
    files = glob.glob(os.path.join(folder_path, "*.jsonl"))
    return files[0] if files else None


# ── Field resolver ────────────────────────────────────────────────────────────

def pick(row: dict, *candidates, default=None):
    """
    Return the first value from `row` whose key matches any of `candidates`
    (case-insensitive). Falls back to `default` if none match.
    """
    lower = {k.lower(): v for k, v in row.items()}
    for c in candidates:
        val = lower.get(c.lower())
        if val is not None and str(val).strip() != "":
            return val
    return default


# ── Generic batch runner ──────────────────────────────────────────────────────

def _run_batches(driver, cypher: str, filepath: str):
    with driver.session() as session:
        for batch in read_jsonl_in_batches(filepath):
            session.run(cypher, batch=batch)


# ── Ingestors (field-name-agnostic via pick()) ────────────────────────────────

def ingest_customers(driver, filepath):
    """
    Accepts any field layout that contains a partner/customer ID.
    Common variants: businessPartner, BusinessPartner, customerId, customer_id, partner
    """
    print(f"    Fields detected: {sniff_fields(filepath)}")
    cypher = """
    UNWIND $batch AS row
    WITH row,
         coalesce(row.businessPartner, row.BusinessPartner,
                  row.customerId,      row.customer_id,
                  row.partner,         row.id) AS cid
    WHERE cid IS NOT NULL AND cid <> ""
    MERGE (c:Customer {id: toString(cid)})
    SET c.name     = coalesce(row.businessPartnerFullName, row.fullName, row.name,
                               row.customerName, row.companyName, ""),
        c.category = coalesce(row.businessPartnerCategory, row.category, ""),
        c.industry  = coalesce(row.industry, row.industryCode, "")
    """
    _run_batches(driver, cypher, filepath)


def ingest_products(driver, filepath):
    """
    Accepts: product, productId, material, materialId, itemId, id
    """
    print(f"    Fields detected: {sniff_fields(filepath)}")
    cypher = """
    UNWIND $batch AS row
    WITH row,
         coalesce(row.product, row.productId, row.material,
                  row.materialId, row.itemId,  row.id) AS pid
    WHERE pid IS NOT NULL AND pid <> ""
    MERGE (p:Product {id: toString(pid)})
    SET p.type   = coalesce(row.productType,  row.type,  ""),
        p.group  = coalesce(row.productGroup, row.group, ""),
        p.weight = toFloat(coalesce(row.netWeight, row.weight, row.grossWeight, 0))
    """
    _run_batches(driver, cypher, filepath)


def ingest_sales_orders(driver, filepath):
    """
    Accepts: salesOrder, salesOrderId, orderId, order_id, id
    soldToParty / customerId / customer for the Customer link.
    """
    print(f"    Fields detected: {sniff_fields(filepath)}")
    cypher = """
    UNWIND $batch AS row
    WITH row,
         coalesce(row.salesOrder, row.salesOrderId,
                  row.orderId,    row.order_id, row.id) AS oid,
         coalesce(row.soldToParty, row.customerId,
                  row.customer,   row.soldTo)           AS cid
    WHERE oid IS NOT NULL AND oid <> ""
    MERGE (o:SalesOrder {id: toString(oid)})
    SET o.type      = coalesce(row.salesOrderType, row.orderType, row.type, ""),
        o.netAmount = toFloat(coalesce(row.totalNetAmount, row.netAmount,
                                       row.amount, row.totalAmount, 0)),
        o.currency  = coalesce(row.transactionCurrency, row.currency, ""),
        o.date      = coalesce(row.creationDate, row.orderDate, row.date, ""),
        o.status    = coalesce(row.overallDeliveryStatus, row.status,
                                row.deliveryStatus, "")
    WITH o, cid
    WHERE cid IS NOT NULL AND toString(cid) <> ""
    MERGE (c:Customer {id: toString(cid)})
    MERGE (c)-[:PLACED]->(o)
    """
    _run_batches(driver, cypher, filepath)


def ingest_sales_order_items(driver, filepath):
    """
    Links SalesOrder → Product.
    Accepts: salesOrder+material, orderId+productId, etc.
    """
    print(f"    Fields detected: {sniff_fields(filepath)}")
    cypher = """
    UNWIND $batch AS row
    WITH row,
         coalesce(row.salesOrder, row.salesOrderId,
                  row.orderId,    row.order_id)   AS oid,
         coalesce(row.material,   row.materialId,
                  row.product,    row.productId,
                  row.itemId)                      AS pid
    WHERE oid IS NOT NULL AND oid <> ""
      AND pid IS NOT NULL AND pid <> ""
    MERGE (o:SalesOrder {id: toString(oid)})
    MERGE (p:Product    {id: toString(pid)})
    MERGE (o)-[:CONTAINS]->(p)
    """
    _run_batches(driver, cypher, filepath)


def ingest_deliveries(driver, filepath):
    """
    Creates Delivery nodes and links them to SalesOrders.
    Accepts many common field name conventions for delivery docs.
    """
    print(f"    Fields detected: {sniff_fields(filepath)}")
    cypher = """
    UNWIND $batch AS row
    WITH row,
         coalesce(row.deliveryDocument,  row.deliveryId,
                  row.delivery,          row.shipmentId,
                  row.outboundDelivery,  row.id)              AS did,
         coalesce(row.salesOrder,        row.salesOrderId,
                  row.referenceSdDocument, row.referenceOrder,
                  row.orderId)                                 AS oid,
         coalesce(row.soldToParty,       row.customerId,
                  row.customer,          row.soldTo)           AS cid
    WHERE did IS NOT NULL AND did <> ""
    MERGE (d:Delivery {id: toString(did)})
    SET d.date   = coalesce(row.actualGoodsMovementDate,  row.goodsMovementDate,
                             row.deliveryDate,             row.shippingDate,
                             row.plannedGoodsMovementDate, row.date, ""),
        d.status = coalesce(row.overallGoodsMovementStatus, row.deliveryStatus,
                             row.overallStatus, row.status, "")
    WITH d, oid, cid
    // Link to SalesOrder
    FOREACH (_ IN CASE WHEN oid IS NOT NULL AND toString(oid) <> "" THEN [1] ELSE [] END |
        MERGE (o:SalesOrder {id: toString(oid)})
        MERGE (o)-[:HAS_DELIVERY]->(d)
    )
    WITH d, cid
    // Link to Customer
    FOREACH (_ IN CASE WHEN cid IS NOT NULL AND toString(cid) <> "" THEN [1] ELSE [] END |
        MERGE (c:Customer {id: toString(cid)})
        MERGE (c)-[:RECEIVED]->(d)
    )
    """
    _run_batches(driver, cypher, filepath)


def ingest_billing_documents(driver, filepath):
    """
    Creates BillingDocument nodes and links to Customers.
    Accepts: billingDocument, billingDocumentId, invoiceId, billId, id
    """
    print(f"    Fields detected: {sniff_fields(filepath)}")
    cypher = """
    UNWIND $batch AS row
    WITH row,
         coalesce(row.billingDocument,   row.billingDocumentId,
                  row.invoiceId,         row.invoice,
                  row.billId,            row.id)               AS bid,
         coalesce(row.soldToParty,       row.customerId,
                  row.customer,          row.soldTo,
                  row.billToParty)                              AS cid
    WHERE bid IS NOT NULL AND bid <> ""
    MERGE (b:BillingDocument {id: toString(bid)})
    SET b.type      = coalesce(row.billingDocumentType, row.invoiceType,
                                row.type, ""),
        b.netAmount = toFloat(coalesce(row.totalNetAmount, row.netAmount,
                                        row.amount, row.invoiceAmount, 0)),
        b.currency  = coalesce(row.transactionCurrency, row.currency, ""),
        b.date      = coalesce(row.billingDocumentDate, row.invoiceDate,
                                row.date, "")
    WITH b, cid
    FOREACH (_ IN CASE WHEN cid IS NOT NULL AND toString(cid) <> "" THEN [1] ELSE [] END |
        MERGE (c:Customer {id: toString(cid)})
        MERGE (b)-[:BILLS]->(c)
    )
    """
    _run_batches(driver, cypher, filepath)


def ingest_billing_document_items(driver, filepath):
    """
    Links BillingDocument → SalesOrder via item-level reference fields.
    """
    print(f"    Fields detected: {sniff_fields(filepath)}")
    cypher = """
    UNWIND $batch AS row
    WITH row,
         coalesce(row.billingDocument,   row.billingDocumentId,
                  row.invoiceId,         row.billId)            AS bid,
         coalesce(row.referenceSdDocument, row.salesOrder,
                  row.salesOrderId,        row.referenceOrder,
                  row.orderId)                                   AS oid
    WHERE bid IS NOT NULL AND bid <> ""
      AND oid IS NOT NULL AND oid <> ""
    MERGE (b:BillingDocument {id: toString(bid)})
    MERGE (o:SalesOrder      {id: toString(oid)})
    MERGE (b)-[:REFERENCES]->(o)
    """
    _run_batches(driver, cypher, filepath)


def ingest_journal_entries(driver, filepath):
    """
    Creates JournalEntry nodes and links to BillingDocuments.
    Accepts: accountingDocument, journalEntry, documentNumber, docId, id
    """
    print(f"    Fields detected: {sniff_fields(filepath)}")
    cypher = """
    UNWIND $batch AS row
    WITH row,
         coalesce(row.accountingDocument,  row.journalEntry,
                  row.documentNumber,      row.docId, row.id)   AS jid,
         coalesce(row.accountingDocumentItem, row.lineItem,
                  row.item,                row.lineNumber, "1") AS item,
         coalesce(row.referenceDocument,   row.billingDocument,
                  row.invoiceId,           row.reference)       AS ref
    WHERE jid IS NOT NULL AND jid <> ""
    MERGE (j:JournalEntry {id: toString(jid) + '-' + toString(item)})
    SET j.accountingDocument          = toString(jid),
        j.documentItem                = toString(item),
        j.fiscalYear                  = coalesce(row.fiscalYear, row.year, ""),
        j.glAccount                   = coalesce(row.glAccount, row.glaccount,
                                                  row.accountNumber, ""),
        j.amount                      = toFloat(coalesce(
                                            row.amountInTransactionCurrency,
                                            row.amount, row.debitAmount,
                                            row.creditAmount, 0)),
        j.currency                    = coalesce(row.transactionCurrency,
                                                  row.currency, ""),
        j.amountInCompanyCodeCurrency = toFloat(coalesce(
                                            row.amountInCompanyCodeCurrency,
                                            row.localAmount, 0)),
        j.companyCodeCurrency         = coalesce(row.companyCodeCurrency,
                                                  row.localCurrency, ""),
        j.postingDate                 = coalesce(row.postingDate, row.valueDate,
                                                  row.date, ""),
        j.documentDate                = coalesce(row.documentDate, row.invoiceDate, ""),
        j.documentType                = coalesce(row.accountingDocumentType,
                                                  row.documentType, row.type, ""),
        j.referenceDocument           = coalesce(ref, ""),
        j.costCenter                  = coalesce(row.costCenter, ""),
        j.profitCenter                = coalesce(row.profitCenter, "")
    WITH j, ref
    FOREACH (_ IN CASE WHEN ref IS NOT NULL AND toString(ref) <> "" THEN [1] ELSE [] END |
        MERGE (b:BillingDocument {id: toString(ref)})
        MERGE (b)-[:HAS_JOURNAL_ENTRY]->(j)
    )
    """
    _run_batches(driver, cypher, filepath)


# ── Folder → ingestor mapping (keyword-based, not exact string) ───────────────
#
# Each entry: (keyword_list, ingestor_function, phase)
#   phase 1 = master data nodes first
#   phase 2 = transactional nodes
#   phase 3 = item/relationship data last
#
# A folder name is matched if it contains ANY of the keywords (case-insensitive).
# The FIRST match wins, so order matters — put more-specific keywords first.

FOLDER_RULES = [
    # ── Phase 1: Master data ─────────────────────────────────────────────────
    (["business_partner", "businesspartner", "customer", "partner", "client"],
     ingest_customers, 1),

    (["product", "material", "item_master", "sku"],
     ingest_products, 1),

    # ── Phase 2: Transactional nodes ─────────────────────────────────────────
    (["sales_order_header", "salesorder_header", "order_header",
      "sales_order" , "salesorder", "so_header"],
     ingest_sales_orders, 2),

    (["delivery_header", "delivery_doc", "delivery",
      "outbound_delivery", "shipment"],
     ingest_deliveries, 2),

    (["billing_document_header", "billing_header",
      "billing_document", "billingdocument",
      "invoice_header", "invoice"],
     ingest_billing_documents, 2),

    (["journal_entr", "accounting_document", "accountingdocument",
      "gl_document", "journal"],
     ingest_journal_entries, 2),

    # ── Phase 3: Item / relationship data ────────────────────────────────────
    (["sales_order_item", "salesorder_item", "order_item", "so_item"],
     ingest_sales_order_items, 3),

    (["billing_document_item", "billing_item", "invoice_item"],
     ingest_billing_document_items, 3),
]


def match_folder(folder_name: str):
    """
    Returns (ingestor_fn, phase) for the first matching rule,
    or (None, None) if the folder is not recognised.
    """
    name_lower = folder_name.lower()
    for keywords, fn, phase in FOLDER_RULES:
        if any(kw in name_lower for kw in keywords):
            return fn, phase
    return None, None


# ── Auto-discovery ────────────────────────────────────────────────────────────

def discover_and_ingest(driver, base_dir: str):
    """
    Walk every sub-directory of `base_dir`, match it against FOLDER_RULES,
    and run ingestion in phase order (1 → 2 → 3).
    Folders that don't match any rule are logged and skipped.
    """
    if not os.path.isdir(base_dir):
        print(f"ERROR: Dataset directory not found: {base_dir}")
        sys.exit(1)

    # Collect all sub-directories that contain at least one .jsonl file
    discovered = {}  # folder_path -> (fn, phase)
    unrecognised = []

    for entry in sorted(os.scandir(base_dir), key=lambda e: e.name):
        if not entry.is_dir():
            continue
        jsonl_files = glob.glob(os.path.join(entry.path, "*.jsonl"))
        if not jsonl_files:
            continue  # no data files — skip silently

        fn, phase = match_folder(entry.name)
        if fn is None:
            unrecognised.append(entry.name)
        else:
            discovered[entry.path] = (entry.name, fn, phase)

    if unrecognised:
        print("⚠  Unrecognised folders (no ingestor matched — skipping):")
        for name in unrecognised:
            print(f"   - {name}")
        print()

    if not discovered:
        print("ERROR: No recognised data folders found under:", base_dir)
        sys.exit(1)

    # Run phases in order
    for phase_num in [1, 2, 3]:
        phase_items = [(p, n, fn) for p, (n, fn, ph) in discovered.items() if ph == phase_num]
        if not phase_items:
            continue

        labels = {1: "Master Data", 2: "Transactional Nodes", 3: "Item / Relationships"}
        print(f"=== Phase {phase_num}: {labels[phase_num]} ===")

        for folder_path, folder_name, ingestor in phase_items:
            jsonl_files = glob.glob(os.path.join(folder_path, "*.jsonl"))
            print(f"  [{folder_name}] {len(jsonl_files)} file(s) → {ingestor.__name__}")
            for f in jsonl_files:
                ingestor(driver, f)
            print(f"  [{folder_name}] ✓ done\n")

    print("✅  All ingestion complete!")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Accept an optional CLI argument for the data directory
    if len(sys.argv) > 1:
        base_data_dir = os.path.abspath(sys.argv[1])
    else:
        base_data_dir = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "sap-o2c-data")
        )

    print(f"Dataset root : {base_data_dir}")
    print(f"Neo4j        : {URI}\n")

    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    try:
        setup_constraints(driver)
        discover_and_ingest(driver, base_data_dir)
    finally:
        driver.close()
