import os
import json
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

client = Groq(api_key=os.environ.get("GROQ_API_KEY"))

# ── Full graph schema — keep this in sync with ingest.py ─────────────────────
GRAPH_SCHEMA = """
Nodes:
- Customer       { id: String, name: String, category: String, industry: String }
- Product        { id: String, type: String, group: String, weight: Float }
- SalesOrder     { id: String, type: String, netAmount: Float, currency: String, date: String, status: String }
- Delivery       { id: String, date: String, status: String }
- BillingDocument{ id: String, type: String, netAmount: Float, currency: String, date: String }
- JournalEntry   { id: String, fiscalYear: String, glAccount: String, amount: Float,
                   currency: String, postingDate: String, documentType: String,
                   documentItem: String, referenceDocument: String,
                   amountInCompanyCodeCurrency: Float, companyCodeCurrency: String }

Relationships:
- (Customer)-[:PLACED]->(SalesOrder)
- (SalesOrder)-[:CONTAINS]->(Product)
- (SalesOrder)-[:HAS_DELIVERY]->(Delivery)
- (BillingDocument)-[:BILLS]->(Customer)
- (BillingDocument)-[:REFERENCES]->(SalesOrder)
- (BillingDocument)-[:HAS_JOURNAL_ENTRY]->(JournalEntry)
"""

# ── 1. Domain guardrail ───────────────────────────────────────────────────────
def check_domain_relevance(user_question: str) -> bool:
    """
    Fast, cheap check: is the question about SAP O2C business data?
    Returns True  → proceed to Cypher generation.
    Returns False → reject with a canned message.
    """
    prompt = f"""You are an intent router for an SAP Order-to-Cash data portal.
Reply with exactly one word — YES or NO.

Reply YES if the question is about any of:
  customers, sales orders, deliveries, billing documents, journal entries,
  products, payments, O2C flow, financial amounts, order status, or the
  dataset described above.

Reply NO for anything else: general knowledge, coding help, small talk,
creative writing, math, geography, etc.

Question: {user_question}
Answer (YES or NO):"""

    response = client.chat.completions.create(
        model="llama-3.1-8b-instant",   # fast + cheap for binary routing
        messages=[{"role": "user", "content": prompt}],
        temperature=0.0,
        max_tokens=5,
    )
    answer = response.choices[0].message.content.strip().upper()
    return answer.startswith("YES")


# ── 2. Natural language → Cypher ─────────────────────────────────────────────
def generate_cypher_query(user_question: str, error_feedback: str | None = None) -> str:
    """
    Translates a natural language question into a read-only Cypher query.
    If `error_feedback` is provided the LLM is asked to fix the previous attempt.
    """
    base_instructions = f"""You are an expert Neo4j Cypher engineer.
Translate the user's question into a strictly valid, read-only Cypher query
using ONLY the schema below.

Schema:
{GRAPH_SCHEMA}

Rules:
1. Return ONLY the raw Cypher query — no explanation, no markdown fences.
2. Use only MATCH and RETURN (and WITH, WHERE, ORDER BY, LIMIT as needed).
3. Never use CREATE, MERGE, SET, DELETE, DETACH, REMOVE, or DROP.
4. Respect relationship directions exactly as shown in the schema.
5. Return meaningful attributes, not just IDs.
6. Default LIMIT is 25 unless the question specifies otherwise.
7. For "full flow" / "trace" questions use path patterns, e.g.:
   MATCH (c:Customer)-[:PLACED]->(o:SalesOrder)-[:HAS_DELIVERY]->(d:Delivery),
         (b:BillingDocument)-[:REFERENCES]->(o),
         (b)-[:HAS_JOURNAL_ENTRY]->(j:JournalEntry)
8. For "broken flow" questions use NOT EXISTS sub-patterns, e.g.:
   MATCH (o:SalesOrder) WHERE NOT EXISTS {{ MATCH (b:BillingDocument)-[:REFERENCES]->(o) }}
"""

    if error_feedback:
        user_content = f"""The previous Cypher attempt failed with this error:
{error_feedback}

Please fix the query and return only the corrected Cypher.

Original question: {user_question}"""
    else:
        user_content = f"Question: {user_question}"

    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",   # strong reasoning for query generation
        messages=[
            {"role": "system", "content": base_instructions},
            {"role": "user",   "content": user_content},
        ],
        temperature=0.1,
        max_tokens=512,
    )

    cypher = response.choices[0].message.content.strip()

    # Strip accidental markdown fences
    for fence in ("```cypher", "```"):
        if cypher.startswith(fence):
            cypher = cypher[len(fence):]
    if cypher.endswith("```"):
        cypher = cypher[:-3]

    return cypher.strip()


# ── 3. DB results → natural language ─────────────────────────────────────────
def generate_final_answer(user_question: str, query_results: list) -> str:
    """
    Summarises Neo4j query results into a concise natural-language answer.
    Strictly grounded — no hallucination allowed.
    """
    if not query_results:
        return (
            "No matching records were found in the database for your query. "
            "Try rephrasing or asking about a different entity."
        )

    # Truncate very large result sets to stay within token limits
    results_to_send = query_results[:50]
    truncated = len(query_results) > 50

    prompt = f"""You are a concise data analyst answering questions about SAP Order-to-Cash data.
Use ONLY the database results provided below — do not invent or assume any information.
If the data does not contain enough to answer the question, say so clearly.

User question: {user_question}

Database results ({len(query_results)} rows{', showing first 50' if truncated else ''}):
{json.dumps(results_to_send, default=str, indent=2)}

Instructions:
- Answer directly and concisely.
- Use bullet points or a short table if it improves readability.
- If listing items, group or rank them when the data supports it.
- Do not mention Cypher, Neo4j, or internal system details.
- End with a one-sentence summary if the result set is large.
"""

    response = client.chat.completions.create(
        model="llama-3.1-8b-instant",   # lighter model is fine for summarisation
        messages=[
            {"role": "system", "content": "You are a helpful SAP data analyst."},
            {"role": "user",   "content": prompt},
        ],
        temperature=0.3,
        max_tokens=800,
    )

    return response.choices[0].message.content.strip()