from google.cloud import bigquery
from google.api_core import exceptions as api_core_exceptions
import sys
import textwrap
import argparse

DESTINATION_SCHEMA = [
    bigquery.SchemaField("code", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("product_name", "STRING", mode="NULLABLE"), # Already present, ensuring it's correct
    bigquery.SchemaField("summary", "STRING", mode="NULLABLE"),
]

def ensure_destination_table(client: bigquery.Client, table_id: str):
    """Checks if the destination table exists and creates it if not."""
    try:
        client.get_table(table_id)
        print(f"Table {table_id} already exists.") 
    except api_core_exceptions.NotFound:
        print(f"Table {table_id} not found. Creating...")
        table = bigquery.Table(table_id, schema=DESTINATION_SCHEMA)
        table = client.create_table(table)
        print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

def build_summary_query(source_table: str, destination_table: str) -> str:
    """Builds the BigQuery SQL script with the UDF to generate summaries."""
    return textwrap.dedent(f"""
        CREATE TEMP FUNCTION generate_summary(
            product_name_json STRING, nutriments_json STRING, ingredients_text_str STRING, categories_tags_json STRING, additives_tags_json STRING, nutriscore_grade_str STRING, nova_group_val STRING
        )
        RETURNS STRING
        LANGUAGE js AS r'''
            function extractList(jsonString, prefix, item_extractor) {{
            if (!jsonString) return null;
            let data_list;
            try {{
                const parsed = JSON.parse(jsonString);
                data_list = parsed.list ? parsed.list.map(i => i.element) : parsed;
                if (Array.isArray(data_list) && data_list.length > 0) {{
                    const items = data_list.map(item_extractor).filter(Boolean);
                    if (items.length > 0) {{
                        return items.join(', ');
                    }}
                }}
            }} catch (e) {{ /* Ignore parsing errors */ }}
            return null;
        }}

        const summary_parts = [];

        // Extract the English product name, with a fallback.
        let product_name = "This product";
        try {{
            if (product_name_json) {{
                const name_list = JSON.parse(product_name_json).list;
                if (name_list) {{
                    const en_name_obj = name_list.find(item => item.element && item.element.lang === 'en');
                    if (en_name_obj && en_name_obj.element.text) {{
                        product_name = en_name_obj.element.text;
                    }}
                }}
            }}
        }} catch (e) {{ /* ignore errors, fallback to default */ }}

        // Part 1: Categories
        const category_items = extractList(categories_tags_json, '', item => {{
            return item ? item.replace(/^en:/, '') : null;
        }});
        if (category_items) {{
            summary_parts.push(product_name + " belongs to " + category_items + " category of products.");
        }}

        // Part 2: Ingredients
        const ingredient_items = extractList(ingredients_text_str, '', item => {{
            // Prefer 'en' text, but take any if 'en' is not available.
            if (item && item.text && item.lang === 'en') return item.text;
            return null; // Initially only consider 'en'
        }}) || extractList(ingredients_text_str, '', item => item.text || null); // Fallback to any lang
        if (ingredient_items) {{
            summary_parts.push("Ingredients in " + product_name + " are " + ingredient_items + ".");
        }}

        // Part 3: Additives
        const additive_items = extractList(additives_tags_json, '', item => {{
            return item ? item.replace(/^en:/, '') : null;
        }});
        if (additive_items) {{
            summary_parts.push(product_name + " has the following additives: " + additive_items + ".");
        }}

        // Part 4: Nutriments
        const nutriment_items = extractList(nutriments_json, '', item => {{
            // The item here is the full element wrapper, e.g., {{ "element": {{ "name": "fat", ... }} }}
            const nutriment = item; // The data is directly in the item for nutriments
            console.log("Processing nutriment item: " + JSON.stringify(nutriment)); // Logs to Cloud Logging
            if (nutriment && nutriment.name && nutriment.value && nutriment.unit && nutriment.name !== 'energy') {{
                const name = nutriment.name.replace(/_/g, ' ');
                const capitalized_name = name.charAt(0).toUpperCase() + name.slice(1);
                return `${{capitalized_name}}: ${{nutriment.value}} ${{nutriment.unit}}`; // Note: template literal braces are already escaped
            }}
            return null;
        }});
        if (nutriment_items) {{
            summary_parts.push(product_name + " contains " + nutriment_items + ".");
        }}

        // Part 5: Nutri-Score
        if (nutriscore_grade_str && nutriscore_grade_str.trim() !== '') {{
            const grade = nutriscore_grade_str.toUpperCase();
            summary_parts.push("It has a Nutri-Score grade of " + grade + ".");
        }} else {{
            summary_parts.push("It has an unknown Nutri-Score grade.");
        }}

        // Part 6: Nova Group
        if (nova_group_val && nova_group_val.trim() !== '' && nova_group_val.trim() !== 'null') {{
            summary_parts.push("It has a Nova group classification of " + nova_group_val + ".");
        }} else {{
            summary_parts.push("It has an unknown Nova group classification.");
        }}

        return summary_parts.length > 0 ? summary_parts.join(' ') : "No summary available.";
        '''

        """).strip() + ";"

def build_insert_query(source_table: str, destination_table: str) -> str:
    """Builds the BigQuery SQL script to insert data using the UDF."""
    return textwrap.dedent(f"""
        INSERT INTO `{destination_table}` (code, product_name, summary)
        WITH FilteredProducts AS (
            SELECT
                code,
                product_name,
                product_name.list[OFFSET(0)].element.text AS extracted_product_name,
                nutriments,
                ingredients_text,
                categories_tags,
                additives_tags,
                nutriscore_grade,
                nova_group
            FROM `{source_table}`
            WHERE ARRAY_LENGTH(product_name.list) > 0
              AND ARRAY_LENGTH(nutriments.list) > 0
        )
        SELECT
            code,
            extracted_product_name,
            generate_summary(
                TO_JSON_STRING(product_name),
                TO_JSON_STRING(ARRAY(SELECT el.element FROM UNNEST(nutriments.list) AS el)),
                TO_JSON_STRING(ingredients_text),
                TO_JSON_STRING(categories_tags),
                TO_JSON_STRING(additives_tags),
                nutriscore_grade,
                CAST(nova_group AS STRING)
            ) AS generated_summary
        FROM FilteredProducts;
    """)

def process_product_data(project_id: str, dataset_id: str, source_table_id: str, destination_table_id: str):
    """
    Reads product data, generates summaries, and inserts them into a destination table.

    Args:
        project_id (str): Your Google Cloud project ID.
        dataset_id (str): The ID of your BigQuery dataset.
        source_table_id (str): The ID of the source table.
        destination_table_id (str): The ID of the destination table.
    """
    client = bigquery.Client(project=project_id)
    destination_table_id_str = f"{project_id}.{dataset_id}.{destination_table_id}"
    source_table_id_str = f"{project_id}.{dataset_id}.{source_table_id}"

    ensure_destination_table(client, destination_table_id_str)

    udf_query = build_summary_query(source_table_id_str, destination_table_id_str)
    insert_query = build_insert_query(source_table_id_str, destination_table_id_str)

    # Combine UDF creation and the INSERT statement into a single script.
    full_query_script = f"{udf_query}\n{insert_query}"

    print("--- Full BigQuery Script ---")
    print(full_query_script)
    print("----------------------------")

    try:
        print("Executing BigQuery script to create UDF and insert summaries...")
        query_job = client.query(full_query_script)
        query_job.result()  # Wait for the job to complete

        print(f"Successfully inserted summaries into {destination_table_id_str}.")
        print(f"Job ID: {query_job.job_id}")
        if query_job.num_dml_affected_rows is not None:
            print(f"Number of rows inserted: {query_job.num_dml_affected_rows}")
    
    except api_core_exceptions.GoogleAPICallError as e:
        print(f"An error occurred while executing the BigQuery job: {e}")
        if hasattr(e, 'errors'):
            for error in e.errors:
                print(f"  - Reason: {error.get('reason')}, Message: {error.get('message')}")

def main():
    """Main function to parse arguments and run the data processing."""
    parser = argparse.ArgumentParser(
        description="Process product data from a BigQuery source table into a summary table."
    )
    parser.add_argument(
        "--project-id", default="phonic-raceway-481118-v0", help="Google Cloud project ID."
    )
    parser.add_argument(
        "--dataset-id", default="Product_Staging", help="BigQuery dataset ID."
    )
    parser.add_argument(
        "--source-table", default="product-staging", help="Source table ID."
    )
    parser.add_argument(
        "--destination-table", default="product_summaries_final", help="Destination table ID."
    )

    args = parser.parse_args()

    # --- Diagnostics ---
    print(f"Using Python executable: {sys.executable}")

    process_product_data( # type: ignore
        project_id=args.project_id,
        dataset_id=args.dataset_id,
        source_table_id=args.source_table,
        destination_table_id=args.destination_table,
    )

if __name__ == "__main__": # type: ignore
    main()