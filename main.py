from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

from llm_agent import generate_cypher_query, generate_final_answer, check_domain_relevance

load_dotenv()

app = FastAPI(title="SAP O2C Graph API")

# Setup CORS for the React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict this to the frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Neo4j Setup
NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "password")

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# Write keywords that should never appear in a read-only query
UNSAFE_WRITE_KEYWORDS = [
    "DELETE ",
    "DETACH DELETE",
    "REMOVE ",
    " SET ",
    "CREATE ",
    "DROP ",
    "MERGE ",
]


class QueryRequest(BaseModel):
    question: str


class QueryResponse(BaseModel):
    cypher_query: str
    result_data: list
    natural_language_answer: str


def is_cypher_safe(cypher: str) -> bool:
    """Returns True only if the query contains no write/mutation keywords."""
    query_upper = cypher.upper()
    if "MATCH" not in query_upper:
        return False
    for kw in UNSAFE_WRITE_KEYWORDS:
        if kw in query_upper:
            return False
    return True


def execute_read_query(cypher: str) -> list:
    """Executes a Cypher query and returns a list of dictionaries."""
    try:
        with driver.session() as session:
            result = session.run(cypher)
            return [record.data() for record in result]
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Neo4j Execution Error: {str(e)}")


@app.post("/api/query", response_model=QueryResponse)
async def process_user_query(req: QueryRequest):
    """
    Pipeline:
      1. Guardrail — domain relevance check
      2. LLM generates Cypher
      3. Safety check on Cypher
      4. Execute against Neo4j (with one LLM retry on failure)
      5. LLM summarises results into natural language
    """
    if not req.question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty.")

    try:
        # ── Guardrail: domain relevance ──────────────────────────────────────
        is_relevant = check_domain_relevance(req.question)
        if not is_relevant:
            return QueryResponse(
                cypher_query="N/A",
                result_data=[],
                natural_language_answer=(
                    "I am an AI assistant specifically built to query this SAP "
                    "Order-to-Cash dataset. I cannot answer general knowledge or "
                    "unrelated questions."
                ),
            )

        # ── Step 1: generate Cypher ──────────────────────────────────────────
        cypher_query = generate_cypher_query(req.question)
        print(f"[Cypher - attempt 1]\n{cypher_query}\n")

        # ── Guardrail: Cypher safety ─────────────────────────────────────────
        if not is_cypher_safe(cypher_query):
            raise HTTPException(
                status_code=400,
                detail="Generated query contained unsafe or invalid keywords.",
            )

        # ── Step 2: run against Neo4j (with one retry) ───────────────────────
        db_results = None
        last_error = None

        try:
            db_results = execute_read_query(cypher_query)
        except HTTPException as neo4j_err:
            last_error = neo4j_err.detail
            print(f"[Neo4j error on attempt 1] {last_error}\nRetrying with LLM…")

            # Feed the error back to the LLM for a corrected query
            cypher_query = generate_cypher_query(req.question, error_feedback=last_error)
            print(f"[Cypher - attempt 2]\n{cypher_query}\n")

            if not is_cypher_safe(cypher_query):
                raise HTTPException(
                    status_code=400,
                    detail="Retry query also contained unsafe or invalid keywords.",
                )

            db_results = execute_read_query(cypher_query)  # let this one raise if still broken

        print(f"[DB rows returned] {len(db_results)}  |  preview: {db_results[:2]}\n")

        # ── Step 3: summarise into natural language ──────────────────────────
        final_answer = generate_final_answer(req.question, db_results)

        return QueryResponse(
            cypher_query=cypher_query,
            result_data=db_results,
            natural_language_answer=final_answer,
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
def health_check():
    """Quick liveness probe — also verifies Neo4j connectivity."""
    try:
        with driver.session() as session:
            session.run("RETURN 1")
        neo4j_status = "connected"
    except Exception as e:
        neo4j_status = f"error: {str(e)}"

    return {"status": "ok", "neo4j": neo4j_status}