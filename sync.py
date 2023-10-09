#!/usr/bin/env python3

import logging as log
import json
import requests
from azure.identity import ClientSecretCredential
import hashlib
import csv

# Settings
CLIENT_ID = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
CLIENT_SECRET = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
TENANT_ID = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
SHARED_MAILBOX_EMAIL = 'mail@domain.com'
FOLDER_CONTACTS = 'company contacts test'
CSV_PATH_ANSPRECHPARTNER = 'ansprechpartner_exchange_online.csv'
CSV_PATH_ADRESSEN = 'adressen_exchange_online.csv'
CONTROL_FILE = 'contacts_control.json'
LOG_FILENAME = 'sync.log'


# Function to load the control file
def load_control_file():
    try:
        with open(CONTROL_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return []

# Function to save the control file
def save_control_file(data):
    with open(CONTROL_FILE, 'w') as f:
        json.dump(data, f, indent=2)

# Function to get all contacts from the folder
def get_all_contacts_from_folder(folder_id):
    headers = {
        'Authorization': 'Bearer %s' % token
    }
    endpoint = 'https://graph.microsoft.com/v1.0/users/%s/contactFolders/%s/contacts?$select=id&$top=1000' % (
        SHARED_MAILBOX_EMAIL,
        folder_id
    )
    all_contacts = []
    while endpoint:
        response = requests.get(endpoint, headers=headers).json()
        all_contacts.extend(response['value'])
        # Check if there are more pages of results
        endpoint = response.get('@odata.nextLink', None)
    return all_contacts


# Function to generate an MD5 hash
def generate_md5_hash(entry):
    """Generate MD5 hash from the contact dict entry"""
    return hashlib.md5(
        json.dumps(entry).encode()
    ).hexdigest()


def get_folder_id_by_name(mailbox_email, folder_name):
    headers = {
        'Authorization': 'Bearer %s' % token
    }
    endpoint = 'https://graph.microsoft.com/v1.0/users/%s/contactFolders?$filter=displayName eq \'%s\'' % (
        mailbox_email,
        folder_name
    )
    response = requests.get(endpoint, headers=headers).json()
    
    if 'value' in response and len(response['value']) > 0:
        return response['value'][0]['id']
    else:
        return None


# Function to add or update a contact
def add_or_update_contact(contact_data, folder_id, control_data, all_contacts):
    headers = {
        'Authorization': 'Bearer %s' % token,
        'Content-Type': 'application/json'
    }

    # Generate hash for the current contact
    current_hash = generate_md5_hash(contact_data)
    map_hash.append(current_hash)
    #print("Current HASH: %s" % current_hash)

    check_hash = ([x for x in control_data if x['HASH'] == current_hash] + [None])[0]

    if check_hash:
        # Check if the contact with the associated ID still exists
        existing_contact = next(
            (contact for contact in all_contacts if contact["id"] == check_hash["ID"]),
            None
        )
        if existing_contact:
            if check_hash["HASH"] != current_hash:
                # Update contact if hash is different
                update_endpoint = 'https://graph.microsoft.com/v1.0/users/%s/contactFolders/%s/contacts/%s' % (
                    SHARED_MAILBOX_EMAIL,
                    folder_id,
                    existing_contact['id']
                )
                update_response = requests.patch(update_endpoint, headers=headers, json=contact_data)
                if update_response.status_code == 200:
                    control_data = [
                        {
                            'ID': item['ID'],
                            'HASH': current_hash if item['ID'] == check_hash["ID"] else item['HASH']
                        } for item in control_data
                    ]
                    #print("New control_data: %s" % json.dumps(control_data, indent=2))
                    log.info("Contact %s successfully updated!" % contact_data['displayName'])
                else:
                    log.critical("Error updating contact. Status: %s, Error: %s" % (
                        update_response.status_code,
                        update_response.text
                    ))
            else:
                log.warning("Contact %s already exists and has no changes" % contact_data['displayName'])
        else:
            # Add entry if not existing on exchange
            control_data = [item for item in control_data if item['HASH'] != current_hash]
            add_contact(contact_data, folder_id, control_data, current_hash)
    else:
        # Add the contact if it's not in our control file
        add_contact(contact_data, folder_id, control_data, current_hash)


# Function to map CSV data to Microsoft Graph's contact format
def map_ansprechpartner_csv(csv_data):
    # Check if the record is a separator line or invalid
    if all(v == "--------" for v in csv_data.values()):
        return None

    phones = []
    for phone_field in ['Business', 'Business2', 'BusinessFax']:
        if csv_data.get(phone_field) and len(phones) < 2:  # Limit 2 values
            phones.append(csv_data[phone_field])
    
    last_name = csv_data.get('LastName', "")
    if last_name:
        given_name = last_name.split(' ')[0] if ' ' in last_name else last_name
        surname = ' '.join(last_name.split(' ')[1:]) if ' ' in last_name else ""
    else:
        given_name = ""
        surname = ""
    mapped_data = {
        'displayName': (last_name or "%s %s" % (given_name, surname).strip()).replace(";", ""),
        'givenName': given_name if given_name else None,
        'surname': surname if surname else None,
        'companyName': csv_data.get('Company') if csv_data.get('Company') else None
    }
    
    # Check if the email is not empty before adding it
    if csv_data.get('Email'):
        mapped_data['emailAddresses'] = [{'address': csv_data['Email']}]
    
    if phones:
        mapped_data['businessPhones'] = phones
    
    if csv_data.get('Mobile'):
        mapped_data['mobilePhone'] = csv_data['Mobile']

    # Remove any fields with value None
    mapped_data = {k: v for k, v in mapped_data.items() if v is not None}

    # Validate the fields before returning
    if not mapped_data['displayName']:
        mapped_data['displayName'] = "Unknown"  # or any other default value

    return mapped_data


# Function to map data from the CSV adressen_exchange_online.csv to Microsoft Graph's contact format
def map_adressen_csv(csv_data):
    # Check if the record is a separator line or invalid
    if all(v == "--------" for v in csv_data.values()):
        return None

    # Mapping CSV fields to the contact format
    mapped_data = {
        'displayName': csv_data['USER_ADRAenderungsdatumDat'].replace(";", "") if csv_data.get('USER_ADRAenderungsdatumDat') else None,
        'companyName': csv_data['Company'].replace(";", "") if csv_data.get('Company') else None,
        'address': {
            'street': csv_data['LieferStrasse'] if csv_data.get('LieferStrasse') else None,
            'city': csv_data['LieferOrt'] if csv_data.get('LieferOrt') else None,
            'postalCode': csv_data['LieferPLZ'] if csv_data.get('LieferPLZ') else None,
            'country': csv_data['Lieferland'] if csv_data.get('Lieferland') else None
        },
        'businessPhones': [csv_data['Business']] if csv_data.get('Business') else [],
        'businessFax': csv_data['BusinessFax'] if csv_data.get('BusinessFax') else None,
        'mobilePhone': csv_data['Mobile'] if csv_data.get('Mobile') else None,
        'emailAddresses': [{'address': csv_data['Email']}] if csv_data.get('Email') else [],
        'website': csv_data['Homepage'] if csv_data.get('Homepage') else None
    }
    
    # Remove any fields with value None
    mapped_data = {k: v for k, v in mapped_data.items() if v is not None}

    return mapped_data

# Helper function to add a contact
def add_contact(contact_data, folder_id, control_data, current_hash):
    if len(batch_add) == 20:
        batch_add_request(control_data)
    batch_add.append({
        "id": str(len(batch_add)),
        "url": "/users/%s/contactFolders/%s/contacts" % (
            SHARED_MAILBOX_EMAIL,
            folder_id
        ),
        "method": "POST",
        "body": contact_data,
        "headers": {
            "Content-Type": "application/json"
        },
        "hash": current_hash  # Add hash to batch item
    })

def batch_add_request(control_data):
    headers = {
        'Authorization': 'Bearer %s' % token,
        'Content-Type': 'application/json'
    }
    add_response = requests.post(
        'https://graph.microsoft.com/v1.0/$batch',
        headers=headers,
        json={ "requests": [item for item in batch_add if "url" in item] }
    )
    responses = add_response.json().get('responses', [])
    for i, resp in enumerate(responses):
        status = resp.get('status', 0)
        body = resp.get('body', {})
        if 200 <= status < 300:
            control_data.append({ "ID": body.get("id"), "HASH": batch_add[i]["hash"] })
            log.info("Contact %s successfully added!" % body.get('displayName'))
        else:
            log.critical("Error adding contact. Status: %s, Error: %s" % (
                status,
                resp.get('headers', {}).get('message', 'Unknown error')
            ))
    batch_add.clear()


# Main
def main():

    # Reading the CSV file and adding/updating contacts
    with open(CSV_PATH_ANSPRECHPARTNER, mode='r', encoding='utf-16-le') as csv_file:
        log.info("Processing CSV file %s" % CSV_PATH_ANSPRECHPARTNER)
        reader = csv.reader(csv_file, delimiter=',')
        headers = next(reader)  # Read headers (first row)

        control_data = load_control_file()
        folder_id = get_folder_id_by_name(SHARED_MAILBOX_EMAIL, FOLDER_CONTACTS)
        if not folder_id:
            log.critical("Folder '%s' not found." % FOLDER_CONTACTS)
            exit()
        all_contacts = get_all_contacts_from_folder(folder_id)

        ansprechpartner_data = []
        for i, row in enumerate(reader, start=1):
            if i == 1:  # Skip the second row (index 1)
                continue
            row_dict = dict(zip(headers, row))
            ansprechpartner_data.append(row_dict)

        for csv_data in ansprechpartner_data:
            contact_data = map_ansprechpartner_csv(csv_data)
            # Print mapped object from csv
            #print(json.dumps(contact_data, indent=2))
            if not contact_data:
                continue # next entry
            add_or_update_contact(contact_data, folder_id, control_data, all_contacts)
            save_control_file(control_data)

        if batch_add:
            batch_add_request(control_data)

    # Reading the CSV file adressen_exchange_online.csv and adding/updating contacts
    with open(CSV_PATH_ADRESSEN, mode='r', encoding='utf-16-le') as csv_file:
        log.info("Processing CSV file %s" % CSV_PATH_ADRESSEN)
        reader = csv.reader(csv_file, delimiter=',')
        headers = next(reader)  # Read headers (first row)

        control_data = load_control_file()
        folder_id = get_folder_id_by_name(SHARED_MAILBOX_EMAIL, FOLDER_CONTACTS)
        if not folder_id:
            log.critical("Folder '%s' not found." % FOLDER_CONTACTS)
            exit()
        all_contacts = get_all_contacts_from_folder(folder_id)

        adressen_data = []
        for i, row in enumerate(reader, start=1):
            if i == 1:  # Skip the second row (index 1)
                continue
            row_dict = dict(zip(headers, row))
            adressen_data.append(row_dict)

        for csv_data in adressen_data:
            contact_data = map_adressen_csv(csv_data)
            # Print csv object
            #print(json.dumps(contact_data, indent=2))
            if not contact_data:
                continue # next entry
            add_or_update_contact(contact_data, folder_id, control_data, all_contacts)
            save_control_file(control_data)

        if batch_add:
            batch_add_request(control_data)

    # Check and remove remote contacts from exchange
    if map_hash:
        control_data = load_control_file()
        control_to_remove = [item for item in control_data if item['HASH'] not in map_hash]
        if control_to_remove:
            for item in control_to_remove:
                headers = {
                    'Authorization': 'Bearer %s' % token,
                    'Content-Type': 'application/json'
                }
                endpoint = 'https://graph.microsoft.com/v1.0/users/%s/contactFolders/%s/contacts/%s' % (
                    SHARED_MAILBOX_EMAIL,
                    get_folder_id_by_name(SHARED_MAILBOX_EMAIL, FOLDER_CONTACTS),
                    item['ID']
                )
                response = requests.delete(endpoint, headers=headers)
                if response.status_code == 204:
                    log.info("Contact removed")
                else:
                    log.critical("Error trying to remove contact. Status: %s, Error: %s" % (
                        response.status_code,
                        response.text
                    ))
            # Update local control_data
            control_data = [item for item in control_data if item['HASH'] not in map_hash]
            save_control_file(control_data)


if __name__ == "__main__":
    # variable Initialization
    batch_add = []
    map_hash = []
    log.basicConfig(
        filename=LOG_FILENAME,
        level=log.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%d/%m/%Y %H:%M:%S'
    )
    # Authentication and token retrieval
    credential = ClientSecretCredential(client_id=CLIENT_ID, client_secret=CLIENT_SECRET, tenant_id=TENANT_ID)
    token = credential.get_token('https://graph.microsoft.com/.default').token
    # start
    main()
