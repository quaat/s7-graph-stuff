from gqlalchemy import Memgraph, Node, Relationship
import requests

def connect():
    try:
        db = Memgraph()
        # Clear the existing data in Memgraph
        db.execute("MATCH (n) DETACH DELETE n;")
    except Exception as e:
        print(f"Failed to connect to Memgraph or clear data: {e}")
        return None

    return db

def fetch_data_model(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data model from {url}")
        return None


def escape_quotes(s):
    """Escape single quotes in a string."""
    return s.replace("'", "\\'")

def create_data_model(db, data_model):
    if not data_model:
        print("No data model provided.")
        return

    # Escape single quotes in uri and description
    uri_escaped = escape_quotes(data_model["uri"])
    description_escaped = escape_quotes(data_model["description"])

    # Create the DataModel node with escaped URI and description
    model_query = f"""
    CREATE (model:DataModel {{uri: '{uri_escaped}', description: '{description_escaped}'}})
    RETURN id(model) AS modelId;
    """
    model_result = db.execute_and_fetch(model_query)
    model_id = next(model_result)["modelId"] if model_result else None

    dimensions = data_model.get("dimensions", {})
    for dimension_name, dimension_description in dimensions.items():
        # Escape single quotes in dimension description
        dimension_description_escaped = escape_quotes(dimension_description)
        dimension_query = f"""
        CREATE (dimension:Dimension {{name: '{dimension_name}', description: '{dimension_description_escaped}'}})
        """
        db.execute(dimension_query)

    for prop_name, prop_details in data_model["properties"].items():
        # Escape single quotes in property details
        prop_description_escaped = escape_quotes(prop_details["description"])
        prop_query = f"""
        MATCH (model:DataModel) WHERE id(model) = {model_id}
        CREATE (prop:Property {{name: '{prop_name}', type: '{prop_details["type"]}', description: '{prop_description_escaped}'}})
        WITH prop, model
        CREATE (model)-[:HAS_PROPERTY]->(prop)
        """
        shape = prop_details.get("shape", [])
        for dimension_name in shape:
            prop_query += f"""
            WITH prop
            MATCH (dimension:Dimension {{name: '{dimension_name}'}})
            CREATE (prop)-[:HAS_DIMENSION]->(dimension)
            """
        prop_query += "RETURN id(prop) AS propId;"
        db.execute(prop_query)

url = "http://onto-ns.com/meta/0.1/Person"

# Fetch the data model
data_model = fetch_data_model(url)

# Connect to MemGraph
db = connect()
create_data_model(db, data_model)
