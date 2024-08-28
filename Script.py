import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, func
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError
from colorama import init, Fore, Style
import logging
import coloredlogs
import datetime

# Initialize colorama
init(autoreset=True)

# Setup logging with coloredlogs for console output
log_file = 'process_log.log'
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

coloredlogs.install(level='DEBUG', fmt='%(asctime)s - %(levelname)s - %(message)s')

# Adding a separator line in the log file for each run
separator_line = "=" * 80
logging.info(separator_line)
logging.info("Starting new process run at: " + str(datetime.datetime.now()))
logging.info(separator_line)

# Database setup
DATABASE_URL = "postgresql://devpgamin:9geN2h326Si3BB@hip-stg-connect-pg.postgres.database.azure.com:5432/med-review-old"
# DATABASE_URL = "postgresql://devpgamin:YmyYuv89vYxQ05B@hip-dev-connect-pg.postgres.database.azure.com:5432/nordb"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base = declarative_base()

# Models
class ClaimDocumentReference(Base):
    __tablename__ = "claimDocumentReference"
    documentRefId = Column(Integer, primary_key=True, autoincrement=True)
    claimType = Column(String(50), nullable=False)
    documentName = Column(String(255), nullable=False)

class DocumentFieldReference(Base):
    __tablename__ = "documentFieldReference"
    documentFieldReferenceId = Column(Integer, primary_key=True, autoincrement=True)
    documentRefId = Column(Integer, ForeignKey("claimDocumentReference.documentRefId"), nullable=True)
    fieldName = Column(String(255), nullable=False)
    claimType = Column(String(50), nullable=False)

def process_excel_to_db(excel_file_path):
    session = Session()
    try:
        print(Fore.CYAN + "Starting database transaction...")
        logging.info("Starting database transaction...")
        
        # Load the Excel file
        print(Fore.CYAN + f"Loading Excel file: {excel_file_path}")
        logging.info(f"Loading Excel file: {excel_file_path}")
        df = pd.read_csv(excel_file_path)
        
        # Filter relevant rows based on "claimType" = "SNF"
        df_filtered = df[df["document_name"].notna() & df["field_name"].notna()]
        
        for _, row in df_filtered.iterrows():
            document_name = row["document_name"]
            field_name = row["field_name"]
            
            print(Fore.CYAN + f"\nProcessing document: {document_name}, field: {field_name}")
            logging.info(f"Processing document: {document_name}, field: {field_name}")
            
            # Query to check if document_name exists in ClaimDocumentReference (case-insensitive)
            document_query = (
                session.query(ClaimDocumentReference)
                .filter(func.lower(ClaimDocumentReference.documentName) == document_name.lower())
                .filter(ClaimDocumentReference.claimType == "SNF")
            ).first()
            
            if not document_query:
                # Document name does not exist, so insert it
                new_document = ClaimDocumentReference(
                    documentName=document_name, claimType="SNF"
                )
                session.add(new_document)
                session.flush()  # Ensure that the primary key is generated before proceeding
                document_ref_id = new_document.documentRefId
                print(Fore.BLUE + f"Inserted new document '{document_name}' with documentRefId: {document_ref_id}.")
                logging.info(f"Inserted new document '{document_name}' with documentRefId: {document_ref_id}.")
            else:
                document_ref_id = document_query.documentRefId
                print(Fore.GREEN + f"Found existing documentRefId: {document_ref_id} for document: {document_name}")
                logging.info(f"Found existing documentRefId: {document_ref_id} for document: {document_name}")
                
            # Check if the field_name already exists for this documentRefId in DocumentFieldReference (case-insensitive)
            field_query = (
                session.query(DocumentFieldReference)
                .filter(DocumentFieldReference.documentRefId == document_ref_id)
                .filter(func.lower(func.trim(DocumentFieldReference.fieldName)) == func.lower(func.trim(field_name)))
                .filter(DocumentFieldReference.claimType == "SNF")
            ).first()
            
            if not field_query:
                # Field name does not exist, so insert it
                new_field = DocumentFieldReference(
                    documentRefId=document_ref_id, fieldName=field_name, claimType="SNF"
                )
                session.add(new_field)
                session.flush()  # Ensure that the primary key is generated before committing
                print(Fore.RED + f"Inserted field '{field_name}' for document '{document_name}' with documentFieldReferenceId: {new_field.documentFieldReferenceId}.")
                logging.info(f"Inserted field '{field_name}' for document '{document_name}' with documentFieldReferenceId: {new_field.documentFieldReferenceId}.")
            else:
                print(Fore.YELLOW + f"Field '{field_name}' already exists for document '{document_name}' with documentFieldReferenceId: {field_query.documentFieldReferenceId}.")
                logging.info(f"Field '{field_name}' already exists for document '{document_name}' with documentFieldReferenceId: {field_query.documentFieldReferenceId}.")
        
        # Commit the changes to the database
        session.commit()
        print(Fore.CYAN + "Transaction committed successfully.")
        logging.info("Transaction committed successfully.")
    
    except SQLAlchemyError as e:
        error_message = f"Error occurred: {str(e)}. Rolling back the transaction..."
        print(Fore.RED + error_message)
        logging.error(error_message)
        session.rollback()
    
    finally:
        session.close()
        print(Fore.CYAN + "Session closed.")
        logging.info("Session closed.")

# Run the processing function
excel_file_path = "/home/Genzeon/PipeLineSegmentation/DEPLOYED CODE/DataEntry/FieldEntry.csv"
process_excel_to_db(excel_file_path)
