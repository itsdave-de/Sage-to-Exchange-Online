import requests
from azure.identity import ClientSecretCredential

# Settings
CLIENT_ID = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
CLIENT_SECRET = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
TENANT_ID = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
SHARED_MAILBOX_EMAIL = 'mail@domain.com'
FOLDER_CONTACTS = 'company contacts test'
MSGRAPH_URL = 'https://graph.microsoft.com/beta'

# Get token access
credential = ClientSecretCredential(client_id=CLIENT_ID, client_secret=CLIENT_SECRET, tenant_id=TENANT_ID)
token = credential.get_token('https://graph.microsoft.com/.default').token

# Set header authorization
headers = {
    'Authorization': f'Bearer {token}',
}

def get_folder_id_by_name(mailbox_email, folder_name):
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
    print(f'Found {len(all_contacts)} contacts in folder {folder_id}.')
    return all_contacts


# Remove each contact in the folder
for contact in get_all_contacts_from_folder(get_folder_id_by_name(SHARED_MAILBOX_EMAIL, FOLDER_CONTACTS)):
    contact_id = contact['id']
    response = requests.delete(
        f'{MSGRAPH_URL}/users/{SHARED_MAILBOX_EMAIL}/contacts/{contact_id}',
        headers=headers,
    )
    if response.status_code == 204:
        print(f'Contact {contact_id} successfully removed.')
    else:
        print(f'Fail to remove contact {contact_id}. Status code: {response.status_code}')
