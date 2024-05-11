from zeep import Client
import json
import xmltodict
import pandas as pd

## Get Session ID - SOAP with zeep
def get_api_session_id():
    # Define the WSDL url
    wsdl = 'https://example.com/ws/general.asmx?wsdl'

    # Create a new Zeep client
    client = Client(wsdl)

    # Define the method and parameters
    
    method = client.service.CreateUserSessionFromInstance
    params = {
        'userName': 'username',
        'instanceName': 'instance_number',
        'password': 'password'
    }

    # Call the SOAP method
    result = method(**params)

    # Return the result
    return result

## Use Zeep to read Report by ID
token = get_api_session_id()
# print(token)

wsdl = 'https://example.com/ws/Search.asmx?wsdl'

# Create a new Zeep client
client = Client(wsdl)

# Define the method and parameters. These are given by the API's documentation
method = client.service.SearchRecordsByReport
params = {
    'sessionToken': token,
    'reportIdOrGuid':'7168',
    'pageNumber':1
    # 1- 422
}


# Call the SOAP method
result = method(**params)

# Parse the XML data and convert it to a Python dict
data_dict = xmltodict.parse(result)

# Convert the Python dict to JSON
json_data = json.dumps(data_dict)

data = json.loads(json_data)
data


# Turn JSON from XML in Pandas DF
df = pd.DataFrame()
records_data = data["Records"]['Record']
for item in records_data:
    # Check if both Record and Field are in the Record object
    # The alternative is that item only contains Field
    if ('Record' in item.keys()) and ('Field' in item.keys()):
        # Some Record items are lists and some are dictionaries.
        record_and_field_list = []
        if type(item['Record']) == list:
            for column_info in item['Record']:

                for tag in column_info['Field']:
                    if tag['@guid'] in mapping_dict.keys():
                        tag['column_name'] = mapping_dict[tag['@guid']]
                    if 'Users' in tag.keys():
                        username = tag['Users']['User']['@firstName'] + ' ' + tag['Users']['User']['@lastName']
                        temp_tag = {tag['column_name']: username}
                    else:
                        temp_tag = {tag['column_name']: tag['#text']}
                    record_and_field_list.append(temp_tag)

        if type(item['Record']) == dict:

            for column_info in item['Record']['Field']:
                if column_info['@guid'] in mapping_dict.keys():
                    column_info['column_name'] = mapping_dict[column_info['@guid']]
                if 'Users' in column_info.keys():
                    username = column_info['Users']['User']['@firstName'] + ' ' + column_info['Users']['User']['@lastName']
                    temp_column_info = {column_info['column_name']: username}
                else:
                    temp_column_info = {column_info['column_name']: column_info['#text']}
                record_and_field_list.append(temp_column_info)

            for column_info in item['Field']:
                if column_info['@guid'] in mapping_dict.keys():
                    column_info['column_name'] = mapping_dict[column_info['@guid']]
                if 'ListValues' in column_info.keys():
                    list_value = column_info['ListValues']['ListValue']['#text']
                    temp_column_info = {column_info['column_name']: list_value}
                else:
                    if '#text' in column_info.keys():
                        temp_column_info = {column_info['column_name']: column_info['#text']}
                    else:
                        temp_column_info = {column_info['column_name']: " "}
                record_and_field_list.append(temp_column_info)

        # Combine dictionaries into one
        combined_dict = {k: v for d in record_and_field_list for k, v in d.items()}
        temp_df = pd.DataFrame([combined_dict])
        df = df.append(temp_df, ignore_index=True)
    else:
        list_field = []
        for column_info in item['Field']:
            if column_info['@guid'] in mapping_dict.keys():
                    column_info['column_name'] = mapping_dict[column_info['@guid']]
            if 'ListValues' in column_info.keys():
                list_value = column_info['ListValues']['ListValue']['#text']
                temp_column_info = {column_info['column_name']: list_value}
            else:
                if '#text' in column_info.keys():
                    temp_column_info = {column_info['column_name']: column_info['#text']}
                else:
                    temp_column_info = {column_info['column_name']: " "}
            list_field.append(temp_column_info)
        # Combine dictionaries into one
        combined_dict = {k: v for d in list_field for k, v in d.items()}
        temp_df = pd.DataFrame([combined_dict])
        df = df.append(temp_df, ignore_index=True)
print(df)

## Terminate Session using Zeep

def terminate_api_session_id():
    # Define the WSDL url
    wsdl = 'https://pfizer.archerirm.us/ws/general.asmx?wsdl'

    # Create a new Zeep client
    client = Client(wsdl)

    # Define the method and parameters
    method = client.service.TerminateSession
    params = {
        'sessionToken': token
    }

    # Call the SOAP method
    result = method(**params)

    # Return the result
    # If termination is successful the result will be 1
    return result
  
terminate_session = terminate_api_session_id()
