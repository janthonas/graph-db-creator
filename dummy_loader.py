from neo4j import GraphDatabase
import pandas as pd

# This file loads the contents of a csv into a Neo4j database
# It does so by:
# - Connecting to the database
# - Importing the csv file
# - Creating a unique list of entities
# - Writes the entities to the database
# - Writes the transactions to the database 

# Connects to the database
URI = "bolt://localhost:7687" 
USERNAME = "neo4j"
PASSWORD = ""

driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))

# Imports the data
imported_df = pd.read_csv('dummy_transactions.csv')

# Creates a unique list of all entities, including remitters and beneficiaries
all_remitters = imported_df[['remitter_sort_code','remitter_account_number','remitter_id', 'remitter_client_name', 'remitter_known_val','remitter_entity_name','remitter_entity_industry']]
all_remitters.columns = ['sort_code', 'account_number', 'id', 'client_name', 'known_val', 'entity_name', 'entity_industry']

all_beneficiaries = imported_df[['beneficiary_sort_code','beneficiary_account_number','beneficiary_id', 'beneficiary_client_name', 'beneficiary_known_val','beneficiary_entity_name','beneficiary_entity_industry']]
all_beneficiaries.columns = ['sort_code', 'account_number', 'id', 'client_name', 'known_val', 'entity_name', 'entity_industry']

all_entities = pd.concat([all_remitters, all_beneficiaries], ignore_index=True)

all_unique_entities = all_entities.drop_duplicates()

# The four following functions are used to creating nodes, alongside creating relationships
def create_node(tx, labels, properties):
    # Ensure labels are formatted correctly (e.g., "Person:Employee")
    labels_str = ":".join(labels) if isinstance(labels, (list, tuple)) else labels
    query = f"CREATE (n:{labels_str} $properties) RETURN n"
    result = tx.run(query, properties=properties)
    return result.single()

def write_node(labels, properties):
    with driver.session() as session:
        node = session.write_transaction(create_node, labels, properties)
        print("Node created:", node["n"])
        
def create_relationship(start_label, start_key, start_value, end_label, end_key, end_value, rel_type, properties=None):
    with driver.session() as session:
        session.write_transaction(
            _create_relationship, start_label, start_key, start_value, 
            end_label, end_key, end_value, rel_type, properties
        )

def _create_relationship(tx, start_label, start_key, start_value, end_label, end_key, end_value, rel_type, properties):
    prop_string = ", ".join(f"{k}: ${k}" for k in (properties or {}).keys())  # Format properties for Cypher
    query = f"""
    MATCH (a:{start_label} {{{start_key}: $start_value}}), 
          (b:{end_label} {{{end_key}: $end_value}})
    MERGE (a)-[r:{rel_type} {{{prop_string}}}]->(b)
    RETURN r
    """
    result = tx.run(query, start_value=start_value, end_value=end_value, **(properties or {}))
    relationship = result.single()
    
    if relationship:
        print("Relationship created:", relationship["r"])
    else:
        print("Failed to create relationship.")
        
# Writes the nodes to the database
for i in range(len(all_unique_entities)):
    write_node(["Entity", all_unique_entities.iloc[i]['entity_name']], {"unique_id": all_unique_entities.iloc[i]['id'], 
                                                                      "sort_code": all_unique_entities.iloc[i]['sort_code'], 
                                                                      "account_number": all_unique_entities.iloc[i]['account_number'], 
                                                                      "known_val": all_unique_entities.iloc[i]['known_val'],
                                                                      "entity_name": all_unique_entities.iloc[i]['entity_name'],
                                                                      "entity_industry": all_unique_entities.iloc[i]['entity_industry']})
    
# Writes the transactions to the database
for j in range(len(imported_df)):
    create_relationship("Entity", "unique_id", imported_df.iloc[j]['remitter_id'], "Entity", "unique_id", imported_df.iloc[j]['beneficiary_id'], "Transaction", {"unique_payment_id": imported_df.iloc[j]['payment_id'],
                                                                                                                                                                 "payment_date_sent": imported_df.iloc[j]['payment_date_sent'],
                                                                                                                                                                 "remitter_client_name": imported_df.iloc[j]['remitter_entity_name'],
                                                                                                                                                                 "beneficiary_client_name": imported_df.iloc[j]['beneficiary_entity_name'],
                                                                                                                                                                 "amount": imported_df.iloc[j]['amount']
                                                                                                                                                                 })