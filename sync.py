#!/usr/bin/env python3

import logging as log
import json
import os
import requests
from azure.identity import ClientSecretCredential
import hashlib
import csv
import sqlite3
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

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

MSGRAPH_URL = 'https://graph.microsoft.com/beta'

# Email settings
EMAIL_SEND = False # Make it True to send email with log file
SENDER_EMAIL = 'your_email@domain.com'
SENDER_PASSWORD = 'your_password'
RECIPIENT_EMAIL = 'recipient_email@domain.com'
SMTP_SERVER = 'smtp.domain.com'
SMTP_PORT = 587

# Function to load the control file
def load_control_file():
    conn = sqlite3.connect(CONTROL_FILE)
    c = conn.cursor()

    # Create table if not exists
    c.execute('''
    CREATE TABLE IF NOT EXISTS control_data (
        id INTEGER PRIMARY KEY,
        contact_id TEXT NOT NULL,
        hash TEXT NOT NULL
    )
    ''')

    # Load existing data
    c.execute('SELECT * FROM control_data')
    control_data = [{'contact_id': row[1], 'hash': row[2]} for row in c.fetchall()]

    conn.close()
    return control_data


# Function to save the control file
def save_control_data(data):
    conn = sqlite3.connect(CONTROL_FILE)
    c = conn.cursor()

    # Insere ou atualiza os dados
    for item in data:
        # Verifica se o registro já existe
        c.execute('SELECT * FROM control_data WHERE contact_id = ?', (item['contact_id'],))
        existing_data = c.fetchone()

        if existing_data:
            # Atualiza o registro se o hash for diferente
            if item['hash'] != existing_data[2]:
                c.execute('UPDATE control_data SET hash = ? WHERE contact_id = ?', (item['hash'], item['contact_id']))
        else:
            # Insere o registro se não existir
            c.execute('INSERT INTO control_data (contact_id, hash) VALUES (?, ?)', (item['contact_id'], item['hash']))

    conn.commit()
    conn.close()


# Function to get all contacts from the folder
def get_all_contacts_from_folder(folder_id):
    headers = {
        'Authorization': 'Bearer %s' % token
    }
    endpoint = '%s/users/%s/contactFolders/%s/contacts?$select=id&$top=1000' % (
        MSGRAPH_URL,
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
    endpoint = '%s/users/%s/contactFolders?$filter=displayName eq \'%s\'' % (
        MSGRAPH_URL,
        mailbox_email,
        folder_name
    )
    response = requests.get(endpoint, headers=headers).json()
    
    if 'value' in response and len(response['value']) > 0:
        return response['value'][0]['id']
    else:
        return None


def add_or_update_contact(contact_data, folder_id, control_data, all_contacts):
    """
    Adds or updates a contact in the specified folder on the Exchange server.

    Args:
        contact_data (dict): The contact data to add or update.
        folder_id (str): The ID of the folder to add or update the contact in.
        control_data (list): A list of dictionaries containing control data for the contacts.
        all_contacts (list): A list of all contacts in the specified folder.

    Returns:
        None
    """
    headers = {
        'Authorization': 'Bearer %s' % token,
        'Content-Type': 'application/json'
    }

    # Generate hash for the current contact
    print(f"Structure of contact_data:\n%s\n-----------------\n" % json.dumps(contact_data, indent=2))
    current_hash = generate_md5_hash(contact_data)
    print(f"Hash generate: {current_hash}\n\n====================\n\n")

    map_hash.append(current_hash)
    #print("Current HASH: %s" % current_hash)
    print(f"Actual size of map_hash: {len(map_hash)}")

    check_hash = ([x for x in control_data if x['hash'] == current_hash] + [None])[0]

    if check_hash:
        # Check if the contact with the associated ID still exists
        existing_contact = next(
            (contact for contact in all_contacts if contact["id"] == check_hash["contact_id"]),
            None
        )
        if existing_contact:
            if check_hash["hash"] != current_hash:
                # Update contact if hash is different
                update_endpoint = '%s/users/%s/contactFolders/%s/contacts/%s' % (
                    MSGRAPH_URL,
                    SHARED_MAILBOX_EMAIL,
                    folder_id,
                    existing_contact['id']
                )
                update_response = requests.patch(update_endpoint, headers=headers, json=contact_data)
                if update_response.status_code == 200:
                    save_control_data([{"contact_id": check_hash["contact_id"], "hash": current_hash}])
                    #print("New control_data: %s" % json.dumps(control_data, indent=2))
                    log.info("Contact %s successfully updated!" % contact_data['displayName'])
                else:
                    log.critical("Error updating contact. Status: %s, Error: %s" % (
                        update_response.status_code,
                        update_response.text
                    ))
            else:
                log.info("%s" % (
                    f"Contact {contact_data['displayName']} already exists and has no changes" if (
                        contact_data.get('displayName') 
                    ) else "Contact already exists and has no changes"
                ))
        else:
            # Add entry if not existing on exchange
            save_control_data([{"contact_id": check_hash["contact_id"], "hash": current_hash}])
            add_contact(contact_data, folder_id, control_data, current_hash)
    else:
        # Add the contact if it's not in our control file
        add_contact(contact_data, folder_id, control_data, current_hash)


# Function to map CSV data to Microsoft Graph's contact format
def map_ansprechpartner_csv(csv_data):
    # Mapping CSV fields to the contact format
    phones = []
    for phone_field in ['Business', 'Business2', 'BusinessFax']:
        if csv_data.get(phone_field) and len(phones) < 2:  # Limit 2 values
            phones.append(
                { 
                    "type": "business",
                    "number": csv_data[phone_field] if csv_data.get(phone_field) else None
                }
            )
    
    if csv_data.get('mobilePhone') and len(phones) < 2:  # Limit 2 values
        phones.append(
            {
                "type": "mobile",
                "number": csv_data['mobilePhone'] if csv_data.get('mobilePhone') else None
            }
        )


    if csv_data.get('Mobile') and len(phones) < 2:  # Limit 2 values
        phones.append(
            {
                "type": "mobile",
                "number": csv_data['Mobile'] if csv_data.get('Mobile') else None
            }
        )

    last_name = csv_data.get('LastName', "") if csv_data.get('LastName') else csv_data.get('FirstName', "")
    if last_name:
        given_name = last_name.split(' ')[0] if ' ' in last_name else last_name
        surname = ' '.join(last_name.split(' ')[1:]) if ' ' in last_name else ""
    else:
        given_name = ""
        surname = ""
    mapped_data = {
        'displayName': (last_name or ("%s %s" % (given_name, surname)).strip()).replace(";", ""),
        'givenName': given_name if given_name else None,
        'surname': surname if surname else None,
        'companyName': csv_data.get('Company') if csv_data.get('Company') else None
    }
    
    # Check if the email is not empty before adding it
    if csv_data.get('Email'):
        mapped_data['emailAddresses'] = [{'address': csv_data['Email']}]
    
    if phones:
        mapped_data['phones'] = phones

    # Remove any fields with value None
    mapped_data = {k: v for k, v in mapped_data.items() if v is not None}

    # Validate the fields before returning
    if not mapped_data['displayName']:
        mapped_data['displayName'] = "Unknown"  # or any other default value

    return mapped_data


# Function to map data from the CSV adressen_exchange_online.csv to Microsoft Graph's contact format
def map_adressen_csv(csv_data):
    # Mapping CSV fields to the contact format
    mapped_data = {
        #'displayName': csv_data['USER_ADRAenderungsdatumDat'].replace(";", "") if csv_data.get('USER_ADRAenderungsdatumDat') else None,
        'companyName': csv_data['Company'].replace(";", "") if csv_data.get('Company') else None,
        'postalAddresses': [{
            'street': csv_data['LieferStrasse'] if csv_data.get('LieferStrasse') else None,
            'city': csv_data['LieferOrt'] if csv_data.get('LieferOrt') else None,
            'postalCode': csv_data['LieferPLZ'] if csv_data.get('LieferPLZ') else None,
            'countryOrRegion': csv_data['Lieferland'] if csv_data.get('Lieferland') else None,
            'type': 'business'
        }] if csv_data.get('LieferStrasse') else None,
        'emailAddresses': [
            {
                'address': csv_data['Email'] if csv_data.get('Email') else None,
                'type': 'work',
                'name': csv_data['Company'] if csv_data.get('Company') else None
            }
        ] if csv_data.get('Email') else [],
        'websites': [
            {
                'type': 'work',
                'address': csv_data['Homepage']
            }
        ] if csv_data.get('Homepage') else [],
    }

    # Check if the phone is not empty before adding it
    phones = []
    for phone_field in ['Business', 'Business2', 'BusinessFax']:
        if csv_data.get(phone_field) and len(phones) < 2:  # Limit 2 values
            phones.append(
                { 
                    "type": "business",
                    "number": csv_data[phone_field] if csv_data.get(phone_field) else None
                }
            )
    # Check if the mobile phone is not empty before adding it
    if csv_data.get('mobilePhone') and len(phones) < 2:  # Limit 2 values
        phones.append(
            {
                "type": "mobile",
                "number": csv_data['mobilePhone'] if csv_data.get('mobilePhone') else None
            }
        )
    # Check if the mobile phone is not empty before adding it
    if csv_data.get('Mobile') and len(phones) < 2:  # Limit 2 values
        phones.append(
            {
                "type": "mobile",
                "number": csv_data['Mobile'] if csv_data.get('Mobile') else None
            }
        )
    # Check if the phone is not empty before adding it
    if phones:
        mapped_data['phones'] = phones

    # Remove any fields with value None
    mapped_data = {k: v for k, v in mapped_data.items() if v is not None}

    return mapped_data


def add_contact(contact_data, folder_id, control_data, current_hash):
    """
    Adds a contact to the specified folder.

    Args:
        contact_data (dict): A dictionary containing the contact's data.
        folder_id (str): The ID of the folder to add the contact to.
        control_data (dict): A dictionary containing control data for the batch request.
        current_hash (str): The current hash value.

    Returns:
        None
    """
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
    """
    Sends a batch request to add multiple contacts to Microsoft Graph API.

    Args:
        control_data (list): A list of dictionaries containing the ID and HASH of each added contact.

    Returns:
        None
    """
    headers = {
        'Authorization': 'Bearer %s' % token,
        'Content-Type': 'application/json'
    }
    add_response = requests.post('%s/$batch' % MSGRAPH_URL,
        headers=headers,
        json={ "requests": [item for item in batch_add if "url" in item] }
    )
    responses = add_response.json().get('responses', [])
    for i, resp in enumerate(responses):
        status = resp.get('status', 0)
        body = resp.get('body', {})
        if 200 <= status < 300:
            save_control_data([{"contact_id": body.get("id"), "hash": batch_add[i]["hash"]}])
            log.info("Contact %s successfully added!" % body.get('displayName'))
        else:
            log.critical("Error adding contact. Status: %s, Error: %s" % (
                status,
                resp.get('headers', {}).get('message', 'Unknown error')
            ))
            print(resp)
    batch_add.clear()

# Function to send email with attachment (log file)
def send_email_with_attachment():
    msg = MIMEMultipart()
    msg['From'] = SENDER_EMAIL
    msg['To'] = RECIPIENT_EMAIL
    msg['Subject'] = 'Notification of execution of syncronization and log activity'

    body = 'Sincronization executed with sucessuful\n\nPlease find attached the log file from the execution of script.'
    msg.attach(MIMEText(body, 'plain'))

    with open(LOG_FILENAME, 'rb') as attachment_file:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment_file.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename={LOG_FILENAME}')
        msg.attach(part)

    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        server.sendmail(SENDER_EMAIL, RECIPIENT_EMAIL, msg.as_string())
        server.quit()
        print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email. Error: {e}")


# Main
def main():

    # Reading the CSV file and adding/updating contacts
    if os.path.exists(CSV_PATH_ANSPRECHPARTNER):
        with open(CSV_PATH_ANSPRECHPARTNER, mode='r', newline='', encoding='utf-16-le') as csv_file:
            log.info("Processing CSV file %s" % CSV_PATH_ANSPRECHPARTNER)
            reader = csv.DictReader(csv_file, delimiter=',')

            control_data = load_control_file()
            folder_id = get_folder_id_by_name(SHARED_MAILBOX_EMAIL, FOLDER_CONTACTS)
            if not folder_id:
                log.critical("Folder '%s' not found." % FOLDER_CONTACTS)
                exit()
            all_contacts = get_all_contacts_from_folder(folder_id)

            ansprechpartner_data = []
            for i, row in enumerate(reader, start=1):
                if row['Company'] != None:
                    ansprechpartner_data.append(row)

            for csv_data in ansprechpartner_data:
                contact_data = map_ansprechpartner_csv(csv_data)
                # Print mapped object from csv
                #print(json.dumps(contact_data, indent=2))
                if not contact_data:
                    continue # next entry
                add_or_update_contact(contact_data, folder_id, control_data, all_contacts)

            if batch_add:
                batch_add_request(control_data)
    else:
        log.warning(f"File {CSV_PATH_ANSPRECHPARTNER} not found.")

            
    # Reading the CSV file adressen_exchange_online.csv and adding/updating contacts
    if os.path.exists(CSV_PATH_ADRESSEN):
        with open(CSV_PATH_ADRESSEN, mode='r', newline='', encoding='utf-16-le') as csv_file:
            log.info("Processing CSV file %s" % CSV_PATH_ADRESSEN)
            reader = csv.DictReader(csv_file, delimiter=',')

            control_data = load_control_file()
            folder_id = get_folder_id_by_name(SHARED_MAILBOX_EMAIL, FOLDER_CONTACTS)
            if not folder_id:
                log.critical("Folder '%s' not found." % FOLDER_CONTACTS)
                exit()
            all_contacts = get_all_contacts_from_folder(folder_id)

            adressen_data = []
            for i, row in enumerate(reader, start=1):
                if row['Company'] != None:
                    adressen_data.append(row)

            for csv_data in adressen_data:
                contact_data = map_adressen_csv(csv_data)
                # Print csv object
                #print(json.dumps(contact_data, indent=2))
                if not contact_data:
                    continue # next entry
                add_or_update_contact(contact_data, folder_id, control_data, all_contacts)

            if batch_add:
                batch_add_request(control_data)
    else:
        log.warning(f"File {CSV_PATH_ADRESSEN} not found.")

    # Check and remove remote contacts from exchange
    if map_hash:
        print(f"Removendo contatos remotos, tamanho atual de map_hash: {len(map_hash)}")
        control_data = load_control_file()
        control_to_remove = [item for item in control_data if item['hash'] not in map_hash]
        print(f"Quantidade de contatos a serem removidos: {len(control_to_remove)}")
        if control_to_remove:
            for item in control_to_remove:
                headers = {
                    'Authorization': 'Bearer %s' % token,
                    'Content-Type': 'application/json'
                }
                endpoint = '%s/users/%s/contactFolders/%s/contacts/%s' % (
                    MSGRAPH_URL,
                    SHARED_MAILBOX_EMAIL,
                    get_folder_id_by_name(SHARED_MAILBOX_EMAIL, FOLDER_CONTACTS),
                    item['ID']
                )
                response = requests.delete(endpoint, headers=headers)
                if response.status_code == 204:
                    log.info(f"Contact with ID {item['contact_id']} removed")
                else:
                    log.critical("Error trying to remove contact. Status: %s, Error: %s" % (
                        response.status_code,
                        response.text
                    ))
            # Update local control_data
            save_control_data(control_to_remove)


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

    # Send email with log file
    if EMAIL_SEND:
        send_email_with_attachment()
