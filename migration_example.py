import csv
import requests
import os

# Zoho CRM API configuration
# (Token is taken from system environment for security)
ZOHO_ACCESS_TOKEN = os.getenv("ZOHO_ACCESS_TOKEN")

ZOHO_API_URL = "https://www.zohoapis.com/crm/v2/Contacts"

headers = {
    "Authorization": f"Zoho-oauthtoken {ZOHO_ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

# ----------------------------
# STEP 1: Extract CSV data
# ----------------------------
def extract_goldmine_contacts(file_path):
    contacts = []
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            contacts.append(row)
    return contacts


# ----------------------------
# STEP 2: Transform data
# ----------------------------
def transform_to_zoho(contact):
    return {
        "First_Name": contact.get("FIRSTNAME"),
        "Last_Name": contact.get("LASTNAME"),
        "Email": contact.get("EMAIL"),
        "Phone": contact.get("PHONE"),
        "Account_Name": contact.get("COMPANY")
    }


# ----------------------------
# STEP 3: Load into Zoho CRM
# ----------------------------
def load_to_zoho(transformed_contacts):
    payload = {"data": transformed_contacts}

    response = requests.post(
        ZOHO_API_URL,
        headers=headers,
        json=payload
    )

    print(response.json())


# ----------------------------
# MAIN ETL RUN
# ----------------------------
def run_etl():
    # IMPORTANT: using sample_data folder path for demo
    goldmine_contacts = extract_goldmine_contacts("sample_data/goldmine_contacts.csv")

    zoho_contacts = []
    for contact in goldmine_contacts:
        zoho_contacts.append(transform_to_zoho(contact))

    load_to_zoho(zoho_contacts)


if __name__ == "__main__":
    run_etl()
