import json
import re
from google.cloud import bigquery
import vertexai # type: ignore
from vertexai.preview.generative_models import GenerativeModel

# 1. Configuration - Update with your details
PROJECT_ID = "phonic-raceway-481118-v0"
LOCATION = "us-central1"
DATASET_ID = "Product_Staging"
TABLE_ID = "product_embeddings_final"
MODEL_PATH = f"`{PROJECT_ID}.{DATASET_ID}.my_text_embedding_model-004`"

# Initialize Clients
vertexai.init(
    project=PROJECT_ID,
    location=LOCATION,
    api_endpoint=f"{LOCATION}-aiplatform.googleapis.com")
bq_client = bigquery.Client(project=PROJECT_ID)
gemini_model = GenerativeModel("gemini-2.5-flash")

def get_candidates(user_query, top_k=25):
    """Stage 1: Broad Vector Search in BigQuery."""
    query_sql = f"""
    SELECT base.product_name, base.summary, base.code
    FROM VECTOR_SEARCH(
      TABLE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`,
      'text_embedding',
      (
        SELECT ml_generate_embedding_result AS text_embedding 
        FROM ML.GENERATE_EMBEDDING(MODEL {MODEL_PATH}, (SELECT @text AS content), STRUCT(TRUE AS flatten_json_output))
      ),
      top_k => {top_k}
    )
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("text", "STRING", user_query)]
    )
    #print(f"Executing query: {query_sql}")
    results = bq_client.query(query_sql, job_config=job_config).to_dataframe()
    return results.to_dict(orient="records")

def rerank_and_filter(user_query, candidates):
    """Stage 2: LLM reasoning to filter against strict constraints."""
    system_prompt = f"""
    You are an expert shopping assistant. I have a list of products retrieved from a search.
    
    User Request: "{user_query}"
    
    Candidates:
    {json.dumps(candidates, indent=2)}
    
    Instructions:
    1. Analyze the user's request. If it contains generic wellness terms (e.g., "healthy", "good", "nutritional"), you MUST base your assessment on the nutritional information and ingredients found in the 'summary' field for each product and nova-group. Do not rely solely on the product's name.
    2. Filter the candidates to keep ONLY those that truly match the user's specific constraints (negations, protein counts, ingredient exclusions).
    3. Rank the filtered candidates by relevance to the user's request.
    4. Return a JSON list of the top 5 products, including their code and name.
    5. Provide a 1-sentence explanation for why each was chosen, referencing the nutritional data if you used it.
    
    Output Format:
    {{"results": [{{"code": "CODE", "name": "PRODUCT_NAME", "explanation": "Reasoning..."}}]}}
    """
    
    response = gemini_model.generate_content(system_prompt)

    # --- Robust JSON Parsing ---
    json_str = None
    # 1. Extract JSON block from markdown
    match = re.search(r'```(json)?\s*(\{.*\})\s*```', response.text, re.DOTALL)
    if match:
        json_str = match.group(2)
    else:
        # 2. Fallback to finding the first and last curly brace
        match = re.search(r'\{.*\}', response.text, re.DOTALL)
        if match:
            json_str = match.group(0)

    if not json_str:
        print("Warning: Could not find a JSON block in the model's response.")
        return None

    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        # 3. Fallback for malformed JSON (e.g., single quotes)
        print("Warning: Model returned malformed JSON. Attempting to fix and re-parse.")
        try:
            # Replace single quotes with double quotes for keys and string values
            fixed_json_str = re.sub(r"'(.*?)'", r'"\1"', json_str)
            return json.loads(fixed_json_str)
        except json.JSONDecodeError as e:
            print(f"Error: Failed to parse JSON even after attempting to fix it. Error: {e}")
            print(f"Original response text:\n---\n{response.text}\n---")
            return None

# --- Main Execution -----
user_input = "Healthy snacks"

print(f"Searching for: {user_input}...")

# Step 1: Broad Recall
candidates = get_candidates(user_input)

# Step 2: Precision Filtering
final_results = rerank_and_filter(user_input, candidates)

if final_results and 'results' in final_results:
    for item in final_results['results']:
        print(f"Code: {item['code']} | Name: {item['name']} | Reason: {item['explanation']}")