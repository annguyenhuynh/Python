import json
import xml.etree.ElementTree as ET
import boto3
import logging
import psycopg2
import re
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize the S3 client
s3_client = boto3.client('s3')

# S3 paths
input_bucket = 'eda-search-documents'
input_prefix = 'contracts/PDS/2025/'
output_bucket = 'eda-search-documents'
output_prefix = 'processed-json-test/'
pdf_prefix = 'contracts/Award/2025/'

def format_date(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d").strftime("%m/%d/%Y") if date_str else None

def extract_contract_number_from_s3_key(s3_key):
    match = re.search(r"([A-Za-z0-9]+)_", s3_key)
    if match:
        return match.group(1)
    return None
    
def get_s3_uri_for_contract(json_data, input_bucket, pdf_prefix):
    contract_number = (
        f"{json_data['enterprise_identifier']}"
        f"{json_data['year']}"
        f"{json_data['type_code']}"
        f"{json_data['serialized_identifier']}"
    )

    # List objects in S3
    response = s3_client.list_objects_v2(Bucket=input_bucket, Prefix=pdf_prefix)

    # Log response structure for debugging
    logger.info(f"Response from S3: {response}")

    if "Contents" not in response:
        logger.info("No files found in the specified S3 location.")
        return None

    # Iterate over objects and check if any match the contract number and is a PDF
    for obj in response["Contents"]:
        # Log the type of obj to understand the structure
        logger.info(f"Processing S3 object: {obj}")

        if isinstance(obj, dict) and "Key" in obj:
            s3_key = obj["Key"]
            
            # Check if the file is a PDF
            if s3_key.lower().endswith(".pdf"):
                # Extract the contract number from the S3 key
                extracted_contract_number = extract_contract_number_from_s3_key(s3_key)
                
                if extracted_contract_number == contract_number:
                    return f"s3://{input_bucket}/{s3_key}"
            else:
                logger.warning(f"Skipping non-PDF file: {s3_key}")
        else:
            logger.warning(f"Unexpected object structure: {obj}")

    return None
    
def insert_json_data(json_data):
    contract_number = (
        f"{json_data['enterprise_identifier']}"
        f"{json_data['year']}"
        f"{json_data['type_code']}"
        f"{json_data['serialized_identifier']}"
    )

    # Get the S3 URI using the contract number
    s3_uri = get_s3_uri_for_contract(json_data, input_bucket, pdf_prefix)

    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            host="eda-contracts-db.c6mhw4szuxuu.us-gov-west-1.rds.amazonaws.com",
            port=5432,
            dbname="postgres",
            user="edaUser",
            password="TGDPza2LI6NgmsHvpzL5"
        )

        if conn.closed == 0:
            logger.info("Connection is open.")
        else:
            logger.error("Connection is closed.")
            return

        cursor = conn.cursor()

        try:
            json_data["contract_number"] = contract_number
            json_data["s3_uri"] = s3_uri

            with conn:
                with conn.cursor() as cur:
                    # Define the INSERT query
                    sql = """
                    INSERT INTO contract_info_test (
                        id,
                        internal_document_number,
                        procurement_instrument_form,
                        enterprise_identifier,
                        year,
                        type_code,
                        serialized_identifier,
                        instrument_vehicle,
                        pricing_arrangement,
                        naics_code,
                        dates_effective_date,
                        dates_period_start,
                        dates_period_end,
                        financial_obligated_amount,
                        financial_total_contract_value,
                        contractor_name,
                        contractor_cage_code,
                        contractor_duns_number,
                        metadata_line_items_count,
                        contract_number,
                        s3_uri,
                        reference_description,
                        reference_value,
                        section
                    )
                    VALUES (
                        %(id)s,
                        %(internal_document_number)s,
                        %(procurement_instrument_form)s,
                        %(enterprise_identifier)s,
                        %(year)s,
                        %(type_code)s,
                        %(serialized_identifier)s,
                        %(instrument_vehicle)s,
                        %(pricing_arrangement)s,
                        %(naics_code)s,
                        %(dates_effective_date)s,
                        %(dates_period_start)s,
                        %(dates_period_end)s,
                        %(financial_obligated_amount)s,
                        %(financial_total_contract_value)s,
                        %(contractor_name)s,
                        %(contractor_cage_code)s,
                        %(contractor_duns_number)s,
                        %(metadata_line_items_count)s,
                        %(contract_number)s,
                        %(s3_uri)s,
                        %(reference_description)s,
                        %(reference_value)s,
                        %(section)s
                    );
                    """
                    cur.execute(sql, json_data)
                    logger.info(f"Rows inserted: {cur.rowcount}")
                    conn.commit()
                    logger.info("Data inserted successfully.")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error inserting data: {e}")
        finally:
            conn.close()

    except Exception as e:
        logger.error(f"Failed to connect to the database: {e}")

	

def extract_id_from_file(file_key):
    parts = file_key.split('/')
    id = parts[-1].split('.')[0]  # Extract the part before the extension
    return id

def find_element(parent, path, default=None):
    try:
        element = parent.find(path)
        if element is not None:
            return element.text
        return default
    except Exception:
        return default

def extract_essential_contract_data(root):
    contract_data = {
        "contract_identification": {},
        "dates": {},
        "financial": {},
        "contractor": {},
        "line_items": [],
        "reference_data":{}
    }

    ns = {"": root.tag.split('}')[0].strip('{') if '}' in root.tag else ""}

    # Extract Contract Identification
    contract_data["contract_identification"] = {
        "internal_document_number": find_element(root, "OriginatorDetails/InternalDocumentNumber"),
        "procurement_instrument_form": find_element(root, "ProcurementInstrumentForm")
    }
    
    # Extract Reference Information
    reference_description = find_element(root, ".//ReferenceDescription")
    reference_value = find_element(root, ".//ReferenceValue")
    section = find_element(root, ".//Section")

    if reference_description:
        contract_data["reference_data"]["reference_description"] = reference_description
    if reference_value:
        contract_data["reference_data"]["reference_value"] = reference_value
    if section:
        contract_data["reference_data"]["section"] = section


    award_instrument = root.find(".//AwardInstrument") or root.find(".//AwardModificationInstrument")
    if award_instrument is None:
        logger.error("Error: Could not find AwardInstrument or AwardModificationInstrument element")
        return None

    header = award_instrument.find(".//ProcurementInstrumentHeader")
    if header is None:
        logger.error("Error: Could not find ProcurementInstrumentHeader element")
        return None

    identifier = header.find(".//ProcurementInstrumentIdentifier/ProcurementInstrumentNumber")
    if identifier is not None:
        contract_data["contract_identification"].update({
            "enterprise_identifier": find_element(identifier, "EnterpriseIdentifier"),
            "year": find_element(identifier, "Year"),
            "type_code": find_element(identifier, "ProcurementInstrumentTypeCode"),
            "serialized_identifier": find_element(identifier, "SerializedIdentifier")
        })

    contract_data["contract_identification"].update({
        "instrument_vehicle": find_element(header, ".//ProcurementInstrumentVehicle"),
        "pricing_arrangement": find_element(header, ".//PricingArrangement/PricingArrangementBase"),
        "naics_code": find_element(header, ".//SolicitationOfferInformation/NAICSCode")
    })

    # Extract Essential Dates
    dates = header.find(".//ProcurementInstrumentDates")
    if dates is not None:
        contract_data["dates"] = {
            "effective_date": format_date(find_element(dates, "ProcurementInstrumentEffectiveDate")),
            "period_start": format_date(find_element(dates, "ProcurementInstrumentPeriods/StartDate")),
            "period_end": format_date(find_element(dates, "ProcurementInstrumentPeriods/EndDate"))
        }

    # Extract Essential Financial Information
    amounts = header.find(".//ProcurementInstrumentAmounts")
    if amounts is not None:
        obligated_amount_elem = amounts.find(".//ObligatedAmounts/ObligatedAmount")
        obligated_amount = obligated_amount_elem.text if obligated_amount_elem is not None else None

        total_value = None
        for other_amount in amounts.findall(".//OtherAmounts"):
            desc_elem = other_amount.find("AmountDescription")
            amount_elem = other_amount.find("Amount")
            if desc_elem is not None and amount_elem is not None:
                if "Total Contract Value" in desc_elem.text:
                    total_value = amount_elem.text
                    break

        contract_data["financial"] = {
            "obligated_amount": obligated_amount,
            "total_contract_value": total_value
        }

    # Extract Essential Contractor Information
    contractor_address = None
    for address_block in header.findall(".//ProcurementInstrumentAddresses"):
        desc_elem = address_block.find("AddressDescription")
        if desc_elem is not None and desc_elem.text == "Contractor":
            contractor_address = address_block
            break

    if contractor_address is not None:
        org_id = contractor_address.find(".//OrganizationID")
        org_name = find_element(contractor_address, ".//OrganizationNameAddress/OrganizationName")

        contract_data["contractor"] = {
            "name": org_name,
            "cage_code": find_element(org_id, "Cage"),
            "duns_number": find_element(org_id, "DunsNumber")
        }

    # Extract Essential Line Item Information
    line_items = []
    for item in award_instrument.findall(".//LineItems"):
        line_item_data = {}

        line_item_data["clin"] = find_element(item, ".//LineItemBase")
        line_item_data["option_line_item"] = find_element(item, ".//OptionLineItem") == "true"
        line_item_data["description"] = find_element(item, ".//ProductServiceDescription")

        unit_price = find_element(item, ".//UnitPrice")
        if unit_price:
            line_item_data["unit_price"] = unit_price

        start_date = find_element(item, ".//LineItemPeriod/PeriodStart/DateElement")
        end_date = find_element(item, ".//LineItemPeriod/PeriodEnd/DateElement")
        if start_date and end_date:
            line_item_data["period"] = {
                "start": start_date,
                "end": end_date
            }

        if line_item_data:
            line_items.append(line_item_data)

    contract_data["line_items"] = line_items
    return contract_data

def flatten_essential_contract_data(contract_data):
    flattened = {}

    if "contract_identification" in contract_data:
        ci = contract_data["contract_identification"]
        flattened["id"] = ci.get("internal_document_number")
        flattened["internal_document_number"] = ci.get("internal_document_number")
        flattened["procurement_instrument_form"] = ci.get("procurement_instrument_form")
        flattened["enterprise_identifier"] = ci.get("enterprise_identifier")
        flattened["year"] = ci.get("year")
        flattened["type_code"] = ci.get("type_code")
        flattened["serialized_identifier"] = ci.get("serialized_identifier")
        flattened["instrument_vehicle"] = ci.get("instrument_vehicle")
        flattened["pricing_arrangement"] = ci.get("pricing_arrangement")
        flattened["naics_code"] = ci.get("naics_code")

    if "dates" in contract_data:
        dates = contract_data["dates"]
        flattened["dates_effective_date"] = dates.get("effective_date")
        flattened["dates_period_start"] = dates.get("period_start")
        flattened["dates_period_end"] = dates.get("period_end")

    if "financial" in contract_data:
        financial = contract_data["financial"]
        flattened["financial_obligated_amount"] = financial.get("obligated_amount")
        flattened["financial_total_contract_value"] = financial.get("total_contract_value")


    if "contractor" in contract_data:
        contractor = contract_data["contractor"]
        flattened["contractor_name"] = contractor.get("name")
        flattened["contractor_cage_code"] = contractor.get("cage_code")
        flattened["contractor_duns_number"] = contractor.get("duns_number")

    if "line_items" in contract_data:
        flattened["metadata_line_items_count"] = len(contract_data["line_items"])
        
    if "reference_data" in contract_data:
        reference_data = contract_data["reference_data"]
        flattened["reference_description"] = reference_data.get("reference_description")
        flattened["reference_value"] = reference_data.get("reference_value")
        flattened["section"] = reference_data.get("section")

    return flattened

processed_files = set()
import re
import xml.etree.ElementTree as ET

def preprocess_xml(xml_content):
    # Replace occurrences of & followed by non-defined entities (like &E) with the safe escape &amp;
    # This will only be a basic cleanup for simple cases.
    xml_content = re.sub(r'&(?![a-zA-Z0-9#]{1,8};)', '&amp;', xml_content)
    return xml_content

def get_processed_ids(conn):
    """
    Fetches all processed internal_document_number values from the database.
    Returns a set of processed IDs for quick lookup.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM contract_info_test")
            return {row[0] for row in cur.fetchall()}  # Using a set for O(1) lookups
    except Exception as e:
        print(f"Error fetching processed IDs: {e}")
        return set()

def process_xml_file(file_key):
    if file_key in processed_files:
        logger.info(f"Skipping already processed file: {file_key}")
        return None
    processed_files.add(file_key)

    try:
        logger.info(f"Processing file: {file_key}")

        # Get the XML content from S3
        response = s3_client.get_object(Bucket=input_bucket, Key=file_key)
        xml_content = response['Body'].read().decode('utf-8')

        # Preprocess the XML content to escape special characters like '&'
        xml_content = preprocess_xml(xml_content)

        try:
            # Try parsing the XML content
            root = ET.fromstring(xml_content)
            logger.info(f"Successfully fetched and parsed: {file_key}")

        except ET.ParseError as e:
            logger.error(f"XML ParseError in {file_key}: {e}")
            # Log a snippet of the problematic XML for debugging
            logger.error(f"Problematic content around line 276, column 231:\n{xml_content[260:300]}")  # Adjust for area of interest
            return None  # Skip this file if XML is invalid

        # Extract and flatten the data
        contract_data = extract_essential_contract_data(root)
        if contract_data is None:
            logger.error(f"Failed to extract data for {file_key}")
            return None

        flattened_data = flatten_essential_contract_data(contract_data)

        # Use the entire file_key as the id (including extension)
        flattened_data['id'] = file_key

        return json.dumps(flattened_data, indent=4)

    except Exception as e:
        logger.error(f"Unexpected error processing {file_key}: {e}")

    return None  # Return None if processing fails


def process_files(s3_client, conn, bucket_name, prefix):
    """
    Scans and processes XML files in the given S3 bucket prefix.
    """
    try:
        # Fetch list of processed document IDs from the database
        processed_ids = get_processed_ids(conn)

        # List all XML files under the given prefix
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' not in response:
            print("No XML files found.")
            return

        for obj in response['Contents']:
            file_key = obj['Key']
            file_name = file_key.split("/")[-1]  # Extract filename
            file_id = file_name.split("_")[1]  # Extract ID from filename

            if file_id in processed_ids:
                print(f"Skipping {file_key} (already processed)")
            else:
                process_xml_file(s3_client, bucket_name, file_key, conn)

    except Exception as e:
        print(f"Error processing files: {e}")
def list_xml_files():
    files = []
    response = s3_client.list_objects_v2(Bucket=input_bucket, Prefix=input_prefix)

    while 'Contents' in response:
        for obj in response['Contents']:
            file_key = obj['Key']
            logger.info(f"Found file in S3: {file_key}")  # Log all files, not just .xml
            if file_key.lower().endswith('.xml'):  # Ensure both .xml and .XML are considered
                files.append(file_key)

        if response.get('IsTruncated'):
            response = s3_client.list_objects_v2(
                Bucket=input_bucket, Prefix=input_prefix,
                ContinuationToken=response['NextContinuationToken']
            )
        else:
            break

    logger.info(f"Total XML files found: {len(files)}")
    return files

def main():
    xml_files = list_xml_files()
    count = 0
    for file_key in xml_files:
        json_data = process_xml_file(file_key)  # This returns a JSON string
        count += 1

        if not json_data:
            logger.warning(f"Skipping file {file_key} due to empty or None JSON data.")
            continue

        try:
            # Parse the JSON string into a dictionary
            data_dict = json.loads(json_data)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON for {file_key}: {e}")
            continue  

        # Insert the data into the database
        insert_json_data(data_dict)

        # Create the S3 output file key
        output_file_key = f"{output_prefix}{data_dict['id']}.json"

        # Upload the JSON data to S3
        try:
            s3_client.put_object(Bucket=output_bucket, Key=output_file_key, Body=json_data)
            logger.info(f"Processed and uploaded: {output_file_key}")
        except Exception as e:
            logger.error(f"Error uploading {output_file_key} to S3: {e}")
            continue

    return count

if __name__ == '__main__':
    count = main()
    logger.info(f"Total processed files: {count}")

