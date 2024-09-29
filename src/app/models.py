from sqlalchemy import create_engine, Column, Integer, String, Float, Table, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from config import DATABASE_URL  # Import the DB config
from lxml import etree
import os
import re

# Initialize the engine and session
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()


# Defining Author table
class Author(Base):
    __tablename__ = 'author'
    aid = Column(Integer, primary_key=True)
    name = Column(String)

# Defining Wrote table (relation between author and paper)
class Wrote(Base):
    __tablename__ = 'wrote'
    aid = Column(Integer, primary_key=True)
    pid = Column(Integer, primary_key=True)

# Defining probabilistic Studentp table
class Studentp(Base):
    __tablename__ = 'studentp'
    aid = Column(Integer, primary_key=True)
    year = Column(Integer)
    probability = Column(Float)

# Defining probabilistic Advisorp table
class Advisorp(Base):
    __tablename__ = 'advisorp'
    aid1 = Column(Integer, primary_key=True)
    aid2 = Column(Integer, primary_key=True)
    probability = Column(Float)

# Defining the Affiliationp table
class Affiliation(Base):
    __tablename__ = 'affiliation'
    aid = Column(Integer, primary_key=True)
    inst = Column(String)

# Defining the Pub table
class Pub(Base):
    __tablename__ = 'pub'
    pid = Column(Integer, primary_key=True)
    year = Column(Integer)


#----------------------------------------------
# Defining NV classes to strore the results of the MarkoViews transformation
def create_nv_table(table_name):
    metadata = MetaData()
    nv_table = Table(
        table_name, metadata,
        Column('aid1', Integer, primary_key=True),
        Column('aid2', Integer, primary_key=True),
        Column('weight', Float),  # Original probability (from views)
        Column('w_0', Float)      # Transformed weight (from MarkoViews)
    )
    metadata.create_all(engine)
    return nv_table

#------------------------------------------------
# Create all tables in the database
def create_tables():
    Base.metadata.create_all(engine)

# Utility function to drop all tables if needed
def drop_tables():
    Base.metadata.drop_all(engine)


#---------------------------------------------------------Future works

 # adding the DBLP dataset (dataset used in the paper)
 # File paths
# dblp_file_path = r'C:\dblp.xml'
# modified_file_path = r'C:\dblp.xml'
#
#
#
# def parse_dblp_xml():
#     try:
#                 # Read the XML content as a string
#         with open(dblp_file_path, 'r', encoding='utf-8') as file:
#             xml_content = file.read()
#
#                 # Ensure the XML declaration is not altered
#         if not xml_content.strip().startswith('<?xml'):
#             xml_content = '<?xml version="1.0" encoding="UTF-8"?>\n' + xml_content
#
#         # Replace known problematic entities with the appropriate characters
#         entities = {
#             '&uuml;': 'ü',
#             '&auml;': 'ä',
#             '&ouml;': 'ö',
#             '&uacute;': 'ú',
#             '&oacute;': 'ó',
#             '&egrave;': 'è',
#             '&eacute;': 'é',
#             '&euml;': 'ë',
#             '&icirc;': 'î',
#             '&iuml;': 'ï',
#             '&aacute;': 'á',
#             '&atilde;': 'ã',
#             '&Aacute;': 'Á',
#             '&Ccedil;': 'Ç',
#             '&aelig;': 'æ',
#             '&iacute;': 'í',
#             '&reg;': '®',
#             '&ccedil;': 'ç',
#             '&amp;': '&',  # Replace ampersand entity
#             '&lt;': '<',  # Replace less than
#             '&gt;': '>',           # Add more replacements as needed
#         }
#
#         for entity, char in entities.items():
#             xml_content = xml_content.replace(entity, char)
#
#                 # Replace standalone '&' with '&amp;' to avoid XML parsing errors
#         xml_content = re.sub(r'&(?!amp;|lt;|gt;|quot;|apos;)', '&amp;', xml_content)
#
#                 # Escape '<' and '>' inside text to avoid XML parsing errors (exclude XML declaration)
#         xml_content = re.sub(r'(?<!<\?xml[^>]*)(<)', '&lt;', xml_content)
#         xml_content = re.sub(r'(?<!<\?xml[^>]*)(>)', '&gt;', xml_content)
#
#                 # Parse the modified XML
#         tree = etree.fromstring(xml_content.encode('utf-8'))
#         root = tree
#
#                 # Process the XML data as needed
#         authors_data = []
#         for publication in root.findall('.//article') + root.findall('.//inproceedings'):
#             for author in publication.findall('author'):
#                 author_name = author.text
#                 authors_data.append(author_name)
#
#         return authors_data
#
#     except Exception as e:
#         print(f"An error occurred while parsing the XML: {e}")
#         return []
#
#
#         # Extract data
# authors = parse_dblp_xml()
# print(authors)
#
#
# # Extract data
# authors = parse_dblp_xml()
# print(authors)
# # dblp_file_path = r'C:\dblp.xml'  # Path to your modified DBLP file
#
#
# def parse_dblp_xml():
#     try:
#         # Read the file content as bytes
#         with open(dblp_file_path, 'rb') as file:
#             xml_content = file.read()
#
#         # Parse the XML content from bytes
#         tree = etree.fromstring(xml_content)
#         root = tree
#
#         authors_data = []
#         wrote_data = []
#
#         # Iterate over each article/inproceedings in the XML
#         for publication in root.findall('.//article') + root.findall('.//inproceedings'):
#             pid = publication.find('title').text  # Simplified; might need to generate a unique ID
#             year = publication.find('year').text if publication.find('year') is not None else 'Unknown'
#
#             # Extract all authors for this publication
#             for author in publication.findall('author'):
#                 author_name = author.text
#                 aid = hash(author_name)  # Simplified; generate a unique ID based on author name
#
#                 # Append author to the list (assuming unique aid)
#                 authors_data.append({'aid': aid, 'name': author_name})
#
#                 # Append the publication relationship to 'wrote' data
#                 wrote_data.append({'aid': aid, 'pid': pid})
#
#         return authors_data, wrote_data
#
#     except Exception as e:
#         print(f"An error occurred while parsing the DBLP XML: {e}")
#         return [], []
#
# # Extract data
# authors, wrote_relations = parse_dblp_xml()








