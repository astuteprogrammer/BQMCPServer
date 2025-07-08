import os
import json
from typing import Dict, Any, List, Optional
from fastmcp import FastMCP
from google.cloud import bigquery
from google.oauth2 import service_account

# Hardcoded configuration
PROJECT_ID = "[Insert your project ID here]"
DATASET_ID = "[Insert your dataset ID here]"

# Initialize FastMCP server
mcp = FastMCP("bigquery-server",
              dependencies=["google-cloud-bigquery",
        "google-auth",
        "google-auth-oauthlib",
        "google-auth-httplib2"])

# Global BigQuery client
_bq_client: Optional[bigquery.Client] = None

def get_bigquery_client() -> bigquery.Client:
    """Initialize BigQuery client with authentication."""
    global _bq_client
    
    if _bq_client is not None:
        return _bq_client
    
    # Method 1: Direct service account file
    service_account_path = "[Insert the path to the service Account JSON file path here]"  # Update this path
    if os.path.exists(service_account_path):
        credentials = service_account.Credentials.from_service_account_file(
            service_account_path,
            scopes=["https://www.googleapis.com/auth/bigquery"]
        )
        _bq_client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
        return _bq_client
    
    # Method 2: Environment variable, you can set this to your path to Json file! 
    if os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
        _bq_client = bigquery.Client(project=PROJECT_ID)
        return _bq_client
    
    # Method 3: Application Default Credentials
    try:
        _bq_client = bigquery.Client(project=PROJECT_ID)
        return _bq_client
    except Exception as e:
        raise Exception(
            "No authentication found. Please either:\n"
            "1. Set the service account path in the code\n"
            "2. Set GOOGLE_APPLICATION_CREDENTIALS environment variable\n"
            "3. Run 'gcloud auth application-default login'"
        )


# @mcp.tool()
# def spices() -> List[str]:
#     """
#     Get a list of spices available in my Kitchen
    
#     Returns:
#         List of spice names
#     """
#     client = get_bigquery_client()
    
#     query = f"""
#     SELECT name
#     FROM `{PROJECT_ID}.{DATASET_ID}.spices`
#     LIMIT 1000
#     """
    
#     try:
#         query_job = client.query(query)
#         results = query_job.result()
        
#         spices = [row.name for row in results]
#         return spices
        
#     except Exception as e:
#         return [f"Error fetching spices: {str(e)}"]

@mcp.tool()
def query_bigquery(query: str) -> str:
    """
    Execute any BigQuery SQL query (SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, etc.)
    
    Args:
        query: SQL query to execute. You can reference tables as just table_name for tables in the default dataset
    
    Returns:
        Query results or execution status
    """
    client = get_bigquery_client()
    
    # Auto-replace table references for convenience
    if f"{PROJECT_ID}.{DATASET_ID}" not in query:
        import re
        patterns = [
            (r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)\b', f'FROM `{PROJECT_ID}.{DATASET_ID}.\\1`'),
            (r'\bINTO\s+([a-zA-Z_][a-zA-Z0-9_]*)\b', f'INTO `{PROJECT_ID}.{DATASET_ID}.\\1`'),
            (r'\bUPDATE\s+([a-zA-Z_][a-zA-Z0-9_]*)\b', f'UPDATE `{PROJECT_ID}.{DATASET_ID}.\\1`'),
            (r'\bTABLE\s+(?!IF\s+EXISTS)([a-zA-Z_][a-zA-Z0-9_]*)\b', f'TABLE `{PROJECT_ID}.{DATASET_ID}.\\1`'),
        ]
        for pattern, replacement in patterns:
            query = re.sub(pattern, replacement, query, flags=re.IGNORECASE)
    
    try:
        # Configure query job
        job_config = bigquery.QueryJobConfig(
            use_query_cache=True,
            use_legacy_sql=False
        )
        
        # Execute query
        query_job = client.query(query, job_config=job_config)
        results = query_job.result()
        
        # Format response based on query type
        query_upper = query.upper().strip()
        
        if query_upper.startswith(('SELECT', 'WITH')):
            # Query returns results
            rows = [dict(row) for row in results]
            
            response = f"Query executed successfully. Found {results.total_rows} rows.\n\n"
            response += "Schema:\n"
            for field in results.schema:
                response += f"  - {field.name}: {field.field_type}\n"
            
            response += "\nResults:\n"
            response += json.dumps(rows, indent=2, default=str)
            
            return response
        else:
            # DDL/DML query
            response = "Query executed successfully."
            if hasattr(query_job, 'num_dml_affected_rows') and query_job.num_dml_affected_rows:
                response += f" Affected {query_job.num_dml_affected_rows} rows."
            return response
            
    except Exception as e:
        return f"Query failed: {str(e)}"

@mcp.tool()
def get_table_schema(table_name: str) -> str:
    """
    Get detailed schema information for a specific table
    
    Args:
        table_name: Name of the table (without project/dataset prefix)
    
    Returns:
        Detailed table schema and metadata
    """
    client = get_bigquery_client()
    
    try:
        table_ref = client.dataset(DATASET_ID).table(table_name)
        table = client.get_table(table_ref)
        
        response = f"Schema for table '{table_name}':\n\n"
        response += f"Total rows: {table.num_rows:,}\n"
        response += f"Size: {table.num_bytes / 1024 / 1024:.2f} MB\n"
        response += f"Created: {table.created}\n"
        response += f"Last modified: {table.modified}\n\n"
        
        response += "Fields:\n"
        for field in table.schema:
            response += f"  - {field.name} ({field.field_type})"
            if field.mode != "NULLABLE":
                response += f" [{field.mode}]"
            if field.description:
                response += f" - {field.description}"
            response += "\n"
        
        # Partitioning info
        if table.time_partitioning:
            response += f"\nPartitioned by: {table.time_partitioning.field} ({table.time_partitioning.type_})\n"
        
        # Clustering info
        if table.clustering_fields:
            response += f"Clustered by: {', '.join(table.clustering_fields)}\n"
        
        return response
        
    except Exception as e:
        return f"Error getting table schema: {str(e)}"

@mcp.tool()
def list_tables() -> str:
    """
    List all tables in the dataset with their metadata
    
    Returns:
        List of all tables with basic information
    """
    client = get_bigquery_client()
    
    try:
        dataset_ref = client.dataset(DATASET_ID)
        tables = list(client.list_tables(dataset_ref))
        
        response = f"Tables in dataset '{DATASET_ID}':\n\n"
        
        for table in tables:
            table_full = client.get_table(table)
            response += f"- {table.table_id}\n"
            response += f"  Type: {table.table_type}\n"
            response += f"  Rows: {table_full.num_rows:,}\n"
            response += f"  Size: {table_full.num_bytes / 1024 / 1024:.2f} MB\n\n"
        
        response += f"Total tables: {len(tables)}"
        
        return response
        
    except Exception as e:
        return f"Error listing tables: {str(e)}"

@mcp.tool()
def get_dataset_info() -> str:
    """
    Get information about the dataset including size, table count, and metadata
    
    Returns:
        Dataset metadata and statistics
    """
    client = get_bigquery_client()
    
    try:
        dataset_ref = client.dataset(DATASET_ID)
        dataset = client.get_dataset(dataset_ref)
        
        response = f"Dataset Information for '{DATASET_ID}':\n\n"
        response += f"Project: {dataset.project}\n"
        response += f"Location: {dataset.location}\n"
        response += f"Created: {dataset.created}\n"
        response += f"Modified: {dataset.modified}\n"
        response += f"Description: {dataset.description or 'No description'}\n"
        
        # Get dataset size using INFORMATION_SCHEMA
        query = f"""
        SELECT 
            COUNT(*) as table_count,
            SUM(size_bytes) / POW(10, 9) as total_size_gb
        FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES__`
        """
        
        query_job = client.query(query)
        results = list(query_job.result())
        
        if results:
            stats = results[0]
            response += f"\nStatistics:\n"
            response += f"- Tables: {stats['table_count']}\n"
            response += f"- Total size: {stats['total_size_gb']:.2f} GB\n"
        
        return response
        
    except Exception as e:
        return f"Error getting dataset info: {str(e)}"


# Run the server
if __name__ == "__main__":
    mcp.run()
