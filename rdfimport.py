import argparse
import rdflib
from rdflib.namespace import RDF, RDFS, OWL
from rdflib.term import BNode
from gqlalchemy import Memgraph

def print_with_border(text):
    """
    Prints the given text within a visually appealing box using UTF-8 box characters.

    Args:
        text (str): The text to print with a border.
    """
    lines = text.split('\n')
    max_length = max(len(line) for line in lines)
    top_border = '╭' + '─' * (max_length + 3) + '╮'
    bottom_border = '╰' + '─' * (max_length + 3) + '╯'
    side_border = '│'

    print(top_border)
    for line in lines:
        print(f"{side_border} {line} {' ' * (max_length - len(line))} {side_border}")
    print(bottom_border)


def parse_ttl_to_cypher(ttl_file_path):
    """
    Parses a TTL (Turtle) file and generates Cypher queries to represent its content in a Memgraph database.

    Args:
        ttl_file_path (str): The path to the Turtle file to be parsed.

    Returns:
        list: A list of Cypher queries as strings.
    """
    g = rdflib.Graph()
    g.parse(ttl_file_path, format="turtle")
    create_queries = []

    # Extract and create queries for named classes, properties, and their relationships
    create_queries.extend(process_named_classes(g))
    create_queries.extend(process_class_hierarchies(g))
    create_queries.extend(process_properties(g, OWL.ObjectProperty, 'ObjectProperty'))
    create_queries.extend(process_properties(g, OWL.DatatypeProperty, 'DatatypeProperty'))
    create_queries.extend(process_properties(g, RDF.Property, 'Property'))
    create_queries.extend(process_property_hierarchies(g))
    create_queries.extend(process_domain_and_range(g))

    return create_queries

def process_named_classes(g):
    """
    Generates Cypher CREATE statements for named classes in the graph.

    Args:
        g (Graph): An rdflib Graph containing the RDF data.

    Returns:
        list: A list of Cypher query strings for creating class nodes.
    """
    queries = []
    for class_type in [RDFS.Class, OWL.Class]:
        for subject in g.subjects(RDF.type, class_type):
            if isinstance(subject, BNode):  # Skip blank nodes
                continue
            label = g.value(subject, RDFS.label) or subject.split('/')[-1]
            queries.append(f"CREATE (:Class {{uri: '{subject}', label: '{label}'}})")
    return queries

def process_class_hierarchies(g):
    """
    Generates Cypher MERGE statements for class hierarchies using rdfs:subClassOf.

    Args:
        g (Graph): An rdflib Graph containing the RDF data.

    Returns:
        list: A list of Cypher query strings for creating subclass relationships.
    """
    queries = []
    for subclass, superclass in g.subject_objects(RDFS.subClassOf):
        if isinstance(subclass, BNode) or isinstance(superclass, BNode):
            continue
        queries.append(
            f"MATCH (child:Class {{uri: '{subclass}'}}), (parent:Class {{uri: '{superclass}'}}) "
            f"MERGE (child)-[:SUBCLASS]->(parent)"
        )
    return queries

def process_properties(g, rdf_type, prop_type):
    """
    Generates Cypher CREATE statements for properties of a specific RDF type.

    Args:
        g (Graph): An rdflib Graph containing the RDF data.
        rdf_type (URIRef): The RDF type of the property (e.g., OWL.ObjectProperty).
        prop_type (str): The label for the property type in the Cypher query.

    Returns:
        list: A list of Cypher query strings for creating property nodes.
    """
    queries = []
    for prop in g.subjects(RDF.type, rdf_type):
        if isinstance(prop, BNode):
            continue
        label = g.value(prop, RDFS.label) or prop.split('/')[-1]
        queries.append(f"CREATE (:Property {{uri: '{prop}', label: '{label}', type: '{prop_type}'}})")
    return queries

def process_property_hierarchies(g):
    """
    Generates Cypher MERGE statements for property hierarchies using rdfs:subPropertyOf.

    Args:
        g (Graph): An rdflib Graph containing the RDF data.

    Returns:
        list: A list of Cypher query strings for creating subproperty relationships.
    """
    queries = []
    for subprop, superprop in g.subject_objects(RDFS.subPropertyOf):
        if isinstance(subprop, BNode) or isinstance(superprop, BNode):
            continue
        queries.append(
            f"MATCH (child:Property {{uri: '{subprop}'}}), (parent:Property {{uri: '{superprop}'}}) "
            f"MERGE (child)-[:SUBPROPERTY]->(parent)"
        )
    return queries

def process_domain_and_range(g):
    """
    Generates Cypher MERGE statements for the domain and range of properties.

    Args:
        g (Graph): An rdflib Graph containing the RDF data.

    Returns:
        list: A list of Cypher query strings for linking properties with their domain and range classes.
    """
    queries = []

    for prop, domain in g.subject_objects(RDFS.domain):
        if isinstance(prop, BNode) or isinstance(domain, BNode):
            continue
        queries.append(
            f"MATCH (prop:Property {{uri: '{prop}'}}), (class:Class {{uri: '{domain}'}}) "
            f"MERGE (prop)-[:DOMAIN]->(class)"
        )

    for prop, range in g.subject_objects(RDFS.range):
        if isinstance(prop, BNode) or isinstance(range, BNode):
            continue
        queries.append(
            f"MATCH (prop:Property {{uri: '{prop}'}}), (class:Class {{uri: '{range}'}}) "
            f"MERGE (prop)-[:RANGE]->(class)"
        )
    return queries

def main():
    """
    Main function to parse command line arguments and run the script.
    """
    parser = argparse.ArgumentParser(description='Convert TTL file to Cypher queries for Memgraph.')
    parser.add_argument('-f', '--file', help='TTL file path', required=True)
    args = parser.parse_args()

    ttl_file_path = args.file
    cypher_queries = parse_ttl_to_cypher(ttl_file_path)

    # Initialize connection to Memgraph
    try:
        db = Memgraph()
        # Clear the existing data in Memgraph
        db.execute("MATCH (n) DETACH DELETE n;")
    except Exception as e:
        print(f"Failed to connect to Memgraph or clear data: {e}")
        return

    # Execute Cypher queries and track success
    successful_creations = 0
    for query in cypher_queries:
        try:
            db.execute(query)
            successful_creations += 1
        except Exception as e:
            print(f"Failed to run query: {e} : `{query}`")

    # Output summary with fancy ASCII border
    summary = f"Summary:\n"
    summary += f"Total Cypher queries generated: {len(cypher_queries)}\n"
    summary += f"Successfully executed queries: {successful_creations}\n"
    summary += f"Failed queries: {len(cypher_queries) - successful_creations}"
    print_with_border(summary)

if __name__ == "__main__":
    main()
