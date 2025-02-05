import pandas as pd
import random

# This file produces a csv file of transactions between entities
# It does so by:
# - Creating the schema of the CSV file
# - Creating a dictionary of entities and properties between them
# - Creating transactions occuring between parties

# These constants define the total number of transactions between parties, and the percent of unknown accounts relative to known accounts
tx_number = 100
unknown_percent = 0.5

# Create an example that stores transactions between parties
payments_df = {
    "payment_id": [],
    "payment_date_sent": [],
    "remitter_sort_code": [],
    "remitter_account_number": [],
    "remitter_id": [],
    "remitter_known_val": [],
    "remitter_entity_name": [],
    "remitter_entity_industry": [],
    "beneficiary_sort_code": [],
    "beneficiary_account_number": [],
    "beneficiary_id": [],
    "beneficiary_known_val": [],
    "beneficiary_entity_name": [],
    "beneficiary_entity_industry": [],
    "amount": []
}

# This turns the 'payments_df' dictionary into a dataframe object which will later become a .csv file
payments_df = pd.DataFrame(payments_df)

# A list of businesses and their respective industries
# This is used to:
# - Generate the various business entities
# - Generate the transactions between the business entities
client_list = [ 
           ['BAE', 'aerospace_defense'],
           ['LockheedMartin', 'aerospace_defense'],
           ['RTX', 'aerospace_defense'],
           ['NorthropGrumman', 'aerospace_defense'],
           ['Boeing', 'aerospace_defense'],
           ['Babcock', 'aerospace_defense'],
           ['GeneralDynamics', 'aerospace_defense'],
           ['Rostec', 'aerospace_defense'],
           ['Airbus', 'aerospace_defense'],
           ['Leonardo', 'aerospace_defense'],
           ['LBG', 'finance'],
           ['Natwest', 'finance'],
           ['Barclays', 'finance'],
           ['HSBC', 'finance'],
           ['Monzo', 'finance'],
           ['Starling', 'finance'],
           ['SSE', 'utilties'],
           ['Octopus', 'utilties'],
           ['Thames_Water', 'utilties'],
           ['Southern_Water', 'utilties'],
           ['BP', 'oilgas'],
           ['Shell', 'oilgas'],
           ['MegaSolar', 'renewables'],
           ['MegaWind', 'renewables'],
           ['AstraZeneca', 'pharma'],
           ['GSK', 'pharma'],
           ['EasyJet', 'travel'],
           ['IHG', 'travel']
]

# Dictionary used to store the entities (entities and accounts are used interchangeably)
accounts = {}

# Lists used to store various identifiers to ensure uniqueness between entities
account_sort_code_list = []
account_number_list = []
client_known_list = []
client_id_list = []

# Generating a list of clients and their respective entities
for client in client_list:
    for i in range(1, 5):
        account_val = client[0].lower() + '_entity_' + str(i)
        account_industry = client[1]
        
        # This is an innefficient way to ensure entity uniqueness
        while True:
            account_sort_code = str(random.randint(10, 99)) + '-' + str(random.randint(10, 99)) + '-' + str(random.randint(10, 99))
            account_number = str(random.randint(10000000, 99999999))
            client_id = str(random.randint(1000000000, 9999999999))
            if (
                account_sort_code not in account_sort_code_list and
                account_number not in account_number_list and
                client_id not in client_id_list
            ):
                break  # Found valid numbers
        client_known = 1
        client_name = client[0]
        entity_name = client[0] + str('_entity_') + str(i)
            
        accounts[account_val] = [account_sort_code, account_number, client_id, client_name, client_known, entity_name, account_industry]
        
        account_sort_code_list.append(account_sort_code)
        account_number_list.append(account_number)
        client_id_list.append(client_id)
        

# Generating a list of unknown clients 50% the length of the list of clients 
for i in range(1, int(len(client_list) * unknown_percent)):
    account_val = 'unknown_' + str(i)
    account_industry = 'unknown'
    
    # This is an innefficient way to ensure entity uniqueness
    while True:
        account_sort_code = str(random.randint(10, 99)) + '-' + str(random.randint(10, 99)) + '-' + str(random.randint(10, 99))
        account_number = str(random.randint(10000000, 99999999))
        client_id = str(random.randint(1000000000, 9999999999))
        if (
            account_sort_code not in account_sort_code_list and
            account_number not in account_number_list and
            client_id not in client_id_list
        ):
            break  # Found valid numbers
    client_name = 'n' + account_sort_code.replace("-", "") + '0' + account_number
    client_known = 0
    entity_name = 'n' + account_sort_code.replace("-", "") + '0' + account_number
    
    accounts[account_val] = [account_sort_code, account_number, client_id, client_name, client_known, entity_name, account_industry]
    
    account_sort_code_list.append(account_sort_code)
    account_number_list.append(account_number)
    client_id_list.append(client_id)

# Returns the number of created entities, should return 125
print(len(accounts))

# Generates a list of transactions had between entites
for tx in range(0, tx_number):
    while True:
        random_first_account = random.choice(list(accounts.keys()))
        random_first_account_val = accounts[random_first_account]

        filtered_account_keys = [key for key in accounts.keys() if key != random_first_account]
        random_second_account = random.choice(list(filtered_account_keys))
        random_second_account_val = accounts[random_second_account]
        
        if(random_first_account_val[3] != 0 or random_second_account_val[3] != 0):
            break # Found valid numbers
        
    payment_amount = random.randint(1, 100000)

    new_row = {
        'payment_id': tx, 
        'payment_date_sent': '05/02/2025',
        'remitter_sort_code': random_first_account_val[0],
        'remitter_account_number': random_first_account_val[1],
        'remitter_id': random_first_account_val[2],
        'remitter_client_name': random_first_account_val[3],
        'remitter_known_val': random_first_account_val[4],
        'remitter_entity_name': random_first_account_val[5],
        'remitter_entity_industry': random_first_account_val[6],
        'beneficiary_sort_code': random_second_account_val[0],
        'beneficiary_account_number': random_second_account_val[1],
        'beneficiary_id': random_second_account_val[2],
        'beneficiary_client_name': random_second_account_val[3],
        'beneficiary_known_val': random_second_account_val[4],
        'beneficiary_entity_name': random_second_account_val[5],
        'beneficiary_entity_industry': random_second_account_val[6],
        'amount': payment_amount
    }
    
    new_row = pd.DataFrame([new_row])
    payments_df = pd.concat([payments_df, new_row], ignore_index=True)

# Saves the transactions to a csv file
payments_df.to_csv("dummy_transactions.csv", index=False)
