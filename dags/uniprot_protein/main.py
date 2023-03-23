from airflow import DAG
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from neo4j import GraphDatabase, basic_auth
import urllib.request
import xml.etree.ElementTree as ET
import os
import xmltodict

dag_path = os.getcwd()

default_args = {
    'owner': 'airflow',
}

@dag(description='ETL of a XML file from Unipro that cointains info about protein', schedule_interval='@daily',catchup=True,start_date=days_ago(1),default_args=default_args)
def etl_uniprot_protein():
    driver = GraphDatabase.driver("bolt://neo4j:7687", auth=basic_auth("neo4j", "weavebio"))

    def write_transaction(query: str, params: dict):
        with driver.session() as session:
            session.execute_write(lambda tx: tx.run(query, **params))
        driver.close()

    @task(task_id='fetch_xml_file')
    def fetch_xml_file():
        url = "https://raw.githubusercontent.com/weavebio/data-engineering-coding-challenge/main/data/Q9Y261.xml"

        # baixar o arquivo XML
        response = urllib.request.urlopen(url)
        xml_data = response.read()
        return xmltodict.parse(xml_data)
    
    @task(task_id='write_accession')
    def write_accession(dict_data):
        accession = dict_data.get("uniprot").get("entry").get("accession")[0]

        query = "CREATE (:Protein {id: $id})"
        params = {
            "id": accession
        }
        write_transaction(query, params)
        
        return accession
    
    @task(task_id='write_full_name')
    def write_full_name(dict_data, accession):
        full_name = dict_data.get("uniprot").get("entry").get("protein").get("recommendedName").get("fullName")
        
        query = "MATCH (p:Protein {id: $id}) CREATE (p)-[:HAS_FULL_NAME]->(:FullName {name: $name})"
        params = {
            "id": accession,
            "name": full_name
        }

        write_transaction(query, params)

    @task(task_id='write_genes')
    def write_genes(dict_data, accession):
        def get_genes(gene):
            return {
                "id": accession,
                "name": gene.get("#text"),
                "status": gene.get('@type')
            }

        genes = dict_data.get("uniprot").get("entry").get("gene").get("name")

        if not isinstance(dict_data.get("uniprot").get("entry").get("gene").get("name"), list):
            genes.append(dict_data.get("uniprot").get("entry").get("gene").get("name"))
        else:
            genes = dict_data.get("uniprot").get("entry").get("gene").get("name")

        genes = list(map(get_genes, genes))

        query = """
            UNWIND $genes AS gene
            MATCH (p:Protein {id: gene.id})
            CREATE (p)-[:FROM_GENE {status: gene.status}]->(:Gene {name: gene.name})
        """
        write_transaction(query, { "genes": genes })

    @task(task_id='get_organisms')
    def get_organisms(dict_data, accession):
        organisms = []

        def get_organism(organism):
            organism_name = next(filter(lambda name: name.get("@type") == "scientific", organism.get("name"))).get("#text")
            organism_common = next(filter(lambda name: name.get("@type") == "common", organism.get("name"))).get("#text")
            taxonomy_id = organism.get("dbReference").get("@id")
            lineages = organism.get("lineage").get("taxon")
            
            return {
                "id": accession,
                "organism_name": organism_name, 
                "organism_common": organism_common, 
                "taxonomy_id": taxonomy_id, 
                "lineages": lineages
            }

        if(isinstance(dict_data.get("uniprot").get("entry").get("organism"), list)):
            organisms = dict_data.get("uniprot").get("entry").get("organism")
        else:
            organisms.append(dict_data.get("uniprot").get("entry").get("organism"))

        organisms = list(map(get_organism, organisms))

        return organisms

    @task(task_id='write_organisms')
    def write_organisms(organisms):
        query = """
            UNWIND $organisms AS organism
            MATCH (p:Protein {id: organism.id})
            CREATE 
                (p)-[:IN_ORGANISM]->
                (:Organism {taxonomy_id: organism.taxonomy_id, name: organism.organism_name, common_name: organism.organism_common})
        """
        write_transaction(query, { "organisms": organisms })

    @task(task_id='write_lineages_of_organisms')
    def write_lineages_of_organisms(organisms):
        def get_lineage(organism, lineage):
            return {
                "taxonomy_id": organism.get("taxonomy_id"),
                "taxon": lineage
            }

        lineages = list(map(lambda organism: list(map(lambda lineage: get_lineage(organism, lineage), organism.get("lineages"))), organisms))
        lineages = [item for sublist in lineages for item in sublist]

        query = """
            UNWIND $lineages AS lineage
            MATCH (o:Organism {taxonomy_id: lineage.taxonomy_id})
            CREATE (o)-[:FROM_LINEAGE]->(:Lineage {taxon: lineage.taxon})
        """
        write_transaction(query, { "lineages": lineages })

    @task(task_id='write_references')
    def write_references(dict_data, accession):
        def get_reference(reference):
            return {
                "type": reference.get("citation").get("@type"),
                "name": reference.get("citation").get("@name"),
                "volume": reference.get("citation").get("@volume"),
                "id": accession
            }

        query = """
            UNWIND $references AS reference
            MATCH (p:Protein {id: reference.id}) 
            CREATE (p)-[:HAS_REFERENCE]->(:Reference {name: reference.name, type: reference.type, volume: reference.volume})
        """

        references = []
        if(isinstance(dict_data.get("uniprot").get("entry").get("reference"), list)):
            references = dict_data.get("uniprot").get("entry").get("reference")
        else:
            references.append(dict_data.get("uniprot").get("entry").get("reference"))

        references = list(map(get_reference, references))

        write_transaction(query, { "references": references })

        return dict_data.get("uniprot").get("entry").get("reference")

    @task(task_id='write_citations_of_references')
    def write_citations_of_references(references):
        def get_citation_from_reference(reference):
            if not reference.get("citation"):
                return []
            
            if(isinstance(reference.get("citation"), list)):
                return list(map(lambda c: { "title": c.get("title"), "reference": reference.get("citation").get("@name") }, reference.get("citation")))
            else:
                return [{
                    "title": reference.get("citation").get("title"), 
                    "reference": reference.get("citation").get("@name")
                }]

        query = """
            UNWIND $citations AS citation
            MATCH (r:Reference {name: citation.reference}) CREATE (r)-[:HAS_CITATION]->(:Citation {title: citation.title})
        """

        citations = list(map(get_citation_from_reference, references))
        citations = [item for sublist in citations for item in sublist]

        write_transaction(query, { "citations": citations })

    @task(task_id='write_authors_of_references')
    def write_authors_of_references(references):
        def get_author_from_reference(reference):
            if not reference.get("citation"):
                return []
            
            if(isinstance(reference.get("citation").get("authorList").get("person"), list)):
                return list(map(lambda author: {
                    "name": author.get("@name"), 
                    "reference": reference.get("citation").get("@name")
                }, reference.get("citation").get("authorList").get("person")))
            else:
                if "person" in reference.get("citation").get("authorList"):
                    return [{"name": reference.get("citation").get("authorList").get("person")}]

                if "consortium" in reference.get("citation").get("authorList"):
                    return [{"name": reference.get("citation").get("authorList").get("consortium")}]

        authors = list(map(get_author_from_reference, references))
        authors = [item for sublist in authors for item in sublist]
                
        query = """
            UNWIND $authors AS author
            MATCH (r:Reference {name: author.reference}) CREATE (r)-[:HAS_AUTHOR]->(:Author {name: author.name})
        """
        write_transaction(query, { "authors": authors })

    @task(task_id='write_features')
    def write_features(dict_data, accession):    
        query = """
            UNWIND $features AS feature
            MATCH (p:Protein {id: feature.id}) 
            CREATE 
                (p)-[:HAS_FEATURE {position_begin: feature.position_begin, position_end: feature.position_end}]->
                (:Feature {description: feature.description, type: feature.type, evidence: feature.evidence})
        """

        def get_feature(feature):
            if "begin" not in feature.get("location"):
                feature["location"] = {
                    "begin": {
                        "@position": feature.get("location").get("position").get("@position")
                    }, 
                    "end": {
                        "@position": feature.get("location").get("position").get("@position")
                    }
                }
                
            return {
                "id": accession,
                "position_begin": feature.get("location").get("begin").get("@position"),
                "position_end": feature.get("location").get("end").get("@position"),
                "description": feature.get("@description"),
                "type": feature.get("@type"),
                "evidence": feature.get("@evidence"),
            }

        features = []

        if not isinstance(dict_data.get("uniprot").get("entry").get("feature"), list):
            features.append(dict_data.get("uniprot").get("entry").get("feature"))
        else:
            features = dict_data.get("uniprot").get("entry").get("feature")

        features = list(map(get_feature, features))
        write_transaction(query, { "features": features })

    @task(task_id='write_evidences')
    def write_evidences(dict_data, accession):  
        query = """
            UNWIND $evidences AS evidence
            MATCH (p:Protein {id: evidence.id}) 
            CREATE 
                (p)-[:HAS_EVIDENCE]->
                (:Evidence {type: evidence.type, key: evidence.key})
        """

        def get_evidence(evidence):
            return {
                "id": accession,
                "type": evidence.get("@type"),
                "key": evidence.get("@key"),
            }

        evidences = []
        if not isinstance(dict_data.get("uniprot").get("entry").get("evidence"), list):
            evidences.append(dict_data.get("uniprot").get("entry").get("evidence"))
        else:
            evidences = dict_data.get("uniprot").get("entry").get("evidence")

        evidences = list(map(get_evidence, evidences))
        write_transaction(query, { "evidences": evidences })

    @task(task_id='write_sequences')
    def write_sequences(dict_data, accession):  
        query = """
            UNWIND $sequences AS sequence
            MATCH (p:Protein {id: sequence.id}) 
            CREATE 
                (p)-[:HAS_SEQUENCE {checksum: sequence.checksum}]->
                (:Sequence {length: sequence.length, mass: sequence.mass, modified: sequence.modified, version: sequence.version, value: sequence.value})
        """

        sequences = []
        if not isinstance(dict_data.get("uniprot").get("entry").get("sequence"), list):
            sequences.append(dict_data.get("uniprot").get("entry").get("sequence"))
        else:
            sequences = dict_data.get("uniprot").get("entry").get("sequence")

        def get_sequence(sequence):
            return {
                "id": accession,
                "length": sequence.get("@length"), 
                "mass": sequence.get("@mass"), 
                "modified": sequence.get("@modified"), 
                "version": sequence.get("@version"), 
                "value": sequence.get("@text"), 
                "checksum": sequence.get("@checksum")
            }

        sequences = list(map(get_sequence, sequences))

        write_transaction(query, { "sequences": sequences })

    dict_data = fetch_xml_file()
    accession = write_accession(dict_data)
    write_full_name(dict_data, accession)
    write_genes(dict_data, accession)

    organisms = get_organisms(dict_data, accession)
    write_organisms(organisms)
    write_lineages_of_organisms(organisms)

    references = write_references(dict_data, accession)
    write_citations_of_references(references)
    write_authors_of_references(references)

    write_features(dict_data, accession)
    write_evidences(dict_data, accession)
    write_sequences(dict_data, accession)

dag = etl_uniprot_protein()