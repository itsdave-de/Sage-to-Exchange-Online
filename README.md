# Exchange Online Contact Synchronization Tool
Integrating CSV Data with Microsoft Graph API

(continue more content)

## JSON Control File

The control file is used to keep track of contacts that have been previously processed and added to the Exchange Online. Each entry in the control file represents a contact and contains two important pieces of information:

 - **ID**: This represents the unique identifier of the contact in Exchange Online.
 - **HASH**: This is an MD5 hash generated from the contact's data. It is used to detect changes in the contact's information.

### How the Control File is Used

#### Loading Existing Contacts

At the beginning of the synchronization process, the control file is read to load all previously processed contacts. This helps the script know which contacts have already been added to Exchange Online and if they have changed since the last sync.

#### Detecting Changes

When processing a contact from the CSV file, the script generates a hash for the contact's data and checks it against the hashes in the control file. If the hash matches an entry in the control file, it means the contact already exists in Exchange Online. The script then decides if the contact needs to be updated (in case the current hash doesn't match the hash in the control file) or if it can be skipped (in case both hashes match).

#### Updating the Control File

 - **For New Contacts**: If a contact from the CSV doesn't have a matching hash in the control file, it's considered a new contact. Once this new contact is successfully added to Exchange Online, a new entry with its ID and hash is appended to the control file.
 - **For Updated Contacts**: If a contact has changed (detected via the hash mismatch), once the contact is successfully updated in Exchange Online, its hash in the control file is updated to the new hash.

#### Cleaning Up

At the end of the script, there's a process to check and remove contacts from Exchange Online that are no longer present in the CSV. If a contact is removed from Exchange Online, its corresponding entry in the control file is also removed.