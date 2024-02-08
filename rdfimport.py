import rdflib
from rdflib.namespace import RDF, RDFS, OWL
from rdflib.term import BNode
from gqlalchemy import Memgraph

import rdflib
from rdflib.namespace import RDF, RDFS, OWL
from rdflib.term import BNode, URIRef

def ttl_to_cypher(ttl_file_path):
    g = rdflib.Graph()
    g.parse(ttl_file_path, format="turtle")

    create_queries = []

    # Process Named Classes
    for class_type in [RDFS.Class, OWL.Class]:
        for subject in g.subjects(RDF.type, class_type):
            if isinstance(subject, BNode):
                continue  # Skip blank nodes
            label = g.value(subject, RDFS.label) or subject.split('/')[-1]
            create_queries.append(f"CREATE (:Class {{uri: '{subject}', label: '{label}'}})")

    # Process Class Hierarchies
    for subclass, superclass in g.subject_objects(RDFS.subClassOf):
        if isinstance(subclass, BNode) or isinstance(superclass, BNode):
            continue
        create_queries.append(
            f"MATCH (child:Class {{uri: '{subclass}'}}), (parent:Class {{uri: '{superclass}'}}) "
            f"MERGE (child)-[:SUBCLASS]->(parent)"
        )

    # Process OWL Object Properties
    for prop in g.subjects(RDF.type, OWL.ObjectProperty):
        if isinstance(prop, BNode):
            continue
        label = g.value(prop, RDFS.label) or prop.split('/')[-1]
        create_queries.append(f"CREATE (:Property {{uri: '{prop}', label: '{label}', type: 'ObjectProperty'}})")

    # Process OWL Datatype Properties
    for prop in g.subjects(RDF.type, OWL.DatatypeProperty):
        if isinstance(prop, BNode):
            continue
        label = g.value(prop, RDFS.label) or prop.split('/')[-1]
        create_queries.append(f"CREATE (:Property {{uri: '{prop}', label: '{label}', type: 'DatatypeProperty'}})")

    # Process RDFS Properties
    for prop in g.subjects(RDF.type, RDF.Property):
        if isinstance(prop, BNode):
            continue
        # This assumes all RDFS.Properties not explicitly marked as OWL Datatype or Object Properties are treated generically.
        # Adjust as necessary for your ontology's specifics.
        label = g.value(prop, RDFS.label) or prop.split('/')[-1]
        create_queries.append(f"CREATE (:Property {{uri: '{prop}', label: '{label}', type: 'Property'}})")
    # Process Property Hierarchies
    for subprop, superprop in g.subject_objects(RDFS.subPropertyOf):
        if isinstance(subprop, BNode) or isinstance(superprop, BNode):
            continue
        create_queries.append(
            f"MATCH (child:Property {{uri: '{subprop}'}}), (parent:Property {{uri: '{superprop}'}}) "
            f"MERGE (child)-[:SUBPROPERTY]->(parent)"
        )

    # Process Domain and Range Information
    for prop, domain in g.subject_objects(RDFS.domain):
        if isinstance(prop, BNode) or isinstance(domain, BNode):
            continue
        create_queries.append(
            f"MATCH (prop:Property {{uri: '{prop}'}}), (class:Class {{uri: '{domain}'}}) "
            f"MERGE (prop)-[:DOMAIN]->(class)"
        )

    for prop, range in g.subject_objects(RDFS.range):
        if isinstance(prop, BNode) or isinstance(range, BNode):
            continue
        create_queries.append(
            f"MATCH (prop:Property {{uri: '{prop}'}}), (class:Class {{uri: '{range}'}}) "
            f"MERGE (prop)-[:RANGE]->(class)"
        )

    return create_queries

# Example usage
ttl_file_path = "vw.ttl"
cypher_queries = ttl_to_cypher(ttl_file_path)


try:
    # Connect to Memgraph
    db = Memgraph()
except Exception as e:
    print(f"Failed to connect to Memgraph: {e}")

try:
    # Clear the existing data in Memgraph
    db.execute("MATCH (n) DETACH DELETE n;")
except Exception as e:
    print(f"Failed to clear data in Memgraph: {e}")

# Output Cypher queries to console or file
for query in cypher_queries:
    try:
        # Clear the existing data in Memgraph
        db.execute(f"{query};")
    except Exception as e:
        print(f"Failed run query: {e} : `{query}`")
