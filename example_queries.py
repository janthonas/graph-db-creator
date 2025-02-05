from neo4j import GraphDatabase


# Neo4j connection details
URI = "bolt://localhost:7687"  # Change if using a remote database
USERNAME = "neo4j"
PASSWORD = ""

# Create a Neo4j driver instance
driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))

# Example Creating a Nodes
def create_node(tx, label, properties):
    query = f"CREATE (n:{label} $properties) RETURN n"
    result = tx.run(query, properties=properties)
    return result.single()

# Function to write a Node
def write_node(label, properties):
    with driver.session() as session:
        node = session.write_transaction(create_node, label, properties)
        print("Node created:", node["n"])
        
        
# Example Deleting Nodes
def delete_node(label, property_key, property_value):
    with driver.session() as session:
        session.write_transaction(_delete_node, label, property_key, property_value)

def _delete_node(tx, label, property_key, property_value):
    query = f"""
    MATCH (n:{label} {{{property_key}: $property_value}})
    DELETE n
    RETURN COUNT(n) AS deleted_count
    """
    result = tx.run(query, property_value=property_value)
    deleted_count = result.single()["deleted_count"]
    if deleted_count == 0:
        print("No matching node found.")
    else:
        print(f"Deleted {deleted_count} node(s).")
        
        
# Example Updating Nodes
def update_node(label, property_key, property_value, updates):
    with driver.session() as session:
        session.write_transaction(_update_node, label, property_key, property_value, updates)

def _update_node(tx, label, property_key, property_value, updates):
    set_clause = ", ".join([f"n.{k} = ${k}" for k in updates.keys()])
    query = f"""
    MATCH (n:{label} {{{property_key}: $property_value}})
    SET {set_clause}
    RETURN n
    """
    result = tx.run(query, property_value=property_value, **updates)
    updated_node = result.single()
    
    if updated_node:
        print("Node updated:", updated_node["n"])
    else:
        print("No matching node found.")
        
        
# Example Creating Relationship
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
    
# Example Deleting a Relationship    
def delete_relationship(start_label, start_key, start_value, end_label, end_key, end_value, rel_type):
    with driver.session() as session:
        session.write_transaction(
            _delete_relationship, start_label, start_key, start_value, 
            end_label, end_key, end_value, rel_type
        )

def _delete_relationship(tx, start_label, start_key, start_value, end_label, end_key, end_value, rel_type):
    query = f"""
    MATCH (a:{start_label} {{{start_key}: $start_value}})-[r:{rel_type}]->(b:{end_label} {{{end_key}: $end_value}})
    DELETE r
    RETURN COUNT(r) AS deleted_count
    """
    result = tx.run(query, start_value=start_value, end_value=end_value)
    deleted_count = result.single()["deleted_count"]
    
    if deleted_count == 0:
        print("No matching relationship found.")
    else:
        print(f"Deleted {deleted_count} relationship(s).")

# Example Updating a Relationship
def update_relationship(start_label, start_key, start_value, end_label, end_key, end_value, rel_type, updates):
    with driver.session() as session:
        session.write_transaction(
            _update_relationship, start_label, start_key, start_value, 
            end_label, end_key, end_value, rel_type, updates
        )

def _update_relationship(tx, start_label, start_key, start_value, end_label, end_key, end_value, rel_type, updates):
    set_clause = ", ".join([f"r.{k} = ${k}" for k in updates.keys()])
    query = f"""
    MATCH (a:{start_label} {{{start_key}: $start_value}})-[r:{rel_type}]->(b:{end_label} {{{end_key}: $end_value}})
    SET {set_clause}
    RETURN r
    """
    result = tx.run(query, start_value=start_value, end_value=end_value, **updates)
    updated_rel = result.single()
    
    if updated_rel:
        print("Relationship updated:", updated_rel["r"])
    else:
        print("No matching relationship found.")

# Example usage
#write_node("Person", {"person_identifier": 0, "name": "John", "age": 25})

# Example usage
# delete_node("Person", "name", "John")

# Example usage: Update a node
# update_node("Person", "name", "Alice", {"age": 50})

# Example: Create a Relationship betweeen two nodes
# create_relationship("Person", "name", "Alice", "Person", "name", "John", "FRIENDS_WITH", {"since": 2022})

# Example: Delete FRIENDS_WITH relationship between Alice and John
# delete_relationship("Person", "name", "Alice", "Person", "name", "John", "FRIENDS_WITH")

# Example: Update FRIENDS_WITH relationship between Alice and John
# update_relationship("Person", "name", "Alice", "Person", "name", "John", "FRIENDS_WITH", {"since": 2056})

# Close the driver connection
driver.close()
