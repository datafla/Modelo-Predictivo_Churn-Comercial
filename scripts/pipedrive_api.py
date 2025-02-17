import requests
import pandas as pd
import re
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
import pandas as pd

# get Metadata - Deals
def deals_metadata(api_token, company_domain):
    base_url = f"https://{company_domain}.pipedrive.com/api/v1/"
    dealfields_url = f"{base_url}dealFields?api_token={api_token}"
    
    try:
        dealfields_response = requests.get(dealfields_url)
        dealfields_response.raise_for_status()  # Validador del request
        dealfields_json = dealfields_response.json()["data"]
        return dealfields_json
    except requests.RequestException as e:
        print(f"HTTP Request failed: {e}")
        return {}
    except KeyError:
        print("Data formatting error - 'data' key not found")
        return {}
    
# get Metadata - Activities
def activities_metadata(api_token, company_domain):
    base_url = f"https://{company_domain}.pipedrive.com/api/v1/"
    activities_url = f"{base_url}activityFields?api_token={api_token}"
    
    try:
        activities_response = requests.get(activities_url)
        activities_response.raise_for_status()  
        activities_json = activities_response.json()["data"]
        return activities_json
    except requests.RequestException as e:
        print(f"HTTP Request failed: {e}")
        return {}
    except KeyError:
        print("Data formatting error - 'data' key not found")
        return {}

# get data with pagination
def get_all_data(url):
    all_data = []
    while True:
        response = requests.get(url)
        response.raise_for_status()  # Validador del request
        data = response.json()
        all_data.extend(data.get('data', []))

        pagination = data.get('additional_data', {}).get('pagination', {})
        if not pagination.get('more_items_in_collection', False):
            break

        next_start = pagination['next_start']
        url = re.sub(r"start=\d+", f"start={next_start}", url)

    return all_data

# get data from Filter - Activities
def get_activities(user_id=None, filter_id=None, type=None, start=None, 
                   limit=None, start_date=None, end_date=None, done=None, 
                   api_token=None, company_domain="api"):
    if api_token is None:
        raise ValueError("API token is required")
    
    base_url = f"https://{company_domain}.pipedrive.com/v1/activities?"
    
    params = {
        "user_id": user_id,
        "filter_id": filter_id,
        "type": type,
        "start": start if start is not None else 0,
        "limit": limit if limit is not None else 500,
        "start_date": start_date,
        "end_date": end_date,
        "done": done,
        "api_token": api_token
    }
    
    query_string = '&'.join(f"{k}={v}" for k, v in params.items() if v is not None)
    full_url = f"{base_url}{query_string}"
    
    return get_all_data(full_url)

def get_deals(user_id=None, filter_id=None, stage_id=None, status=None, 
              start=None, limit=None, sort=None, owned_by_you=None, 
              api_token=None, company_domain="api"):
    if api_token is None:
        raise ValueError("API token is required")
    
    base_url = f"https://{company_domain}.pipedrive.com/v1/deals?"
    
    params = {
        "user_id": user_id,
        "filter_id": filter_id,
        "stage_id": stage_id,
        "status": status,
        "start": start if start is not None else 0,
        "limit": limit if limit is not None else 500,
        "sort": sort,
        "owned_by_you": owned_by_you,
        "api_token": api_token
    }
    
    query_string = '&'.join(f"{k}={v}" for k, v in params.items() if v is not None)
    full_url = f"{base_url}{query_string}"
    
    return get_all_data(full_url)

# get activity fields

def get_activity_fields(api_token, company_domain="api", start=None, limit=None):
    base_url = f"https://{company_domain}.pipedrive.com/v1"
    url = f"{base_url}/activityFields"

    params = {
        'api_token': api_token,
        'start': start if start is not None else 0,
        'limit': limit if limit is not None else 500
    }

    response = requests.get(url, params=params)
    data = response.json()

    if response.status_code == 200 and 'data' in data:
        return data['data']
    else:
        print(f"Error getting fields: {data.get('error', 'Unknown error')}")
        return []
    
# get deals fields
def get_deals_fields(api_token, company_domain="api", start=None, limit=None):
    base_url = f"https://{company_domain}.pipedrive.com/v1"
    url = f"{base_url}/dealFields"

    params = {
        'api_token': api_token,
        'start': start if start is not None else 0,
        'limit': limit if limit is not None else 500
    }
    
    response = requests.get(url, params=params)
    data = response.json()

    if response.status_code == 200 and 'data' in data:
        return data['data']
    else:
        print(f"Error getting fields: {data.get('error', 'Unknown error')}")
        return []
    
# extract Pipeline IDs 
def get_pipeline_id(api_token):
    url = f'https://api.pipedrive.com/v1/pipelines?api_token={api_token}'
    response = requests.get(url)

    if response.status_code == 200:
        pipelines = response.json().get('data', [])
        if pipelines:
            for pipeline in pipelines:
                print(f"Pipeline ID: {pipeline['id']}, Name: {pipeline['name']}")
            return pipelines
        else:
            print("No pipelines found.")
            return None
    else:
        print(f"Error getting pipelines: {response.status_code} - {response.text}")
        return None

# extract Pipeline Stages
def get_stage_info(pipeline_id, api_token):
    url = f'https://api.pipedrive.com/v1/stages?pipeline_id={pipeline_id}&api_token={api_token}'
    response = requests.get(url)
    
    if response.status_code == 200:
        stages = response.json().get('data', [])
        if stages:  
            return stages  
        else:
            print(f"No stages found for pipeline ID {pipeline_id}")
            return None
    else:
        print(f"Error getting stages for pipeline ID {pipeline_id}: {response.status_code} - {response.text}")
        return None
    
def extract_stage_ids_and_names(stages):
    stage_info = {}
    for stage in stages:
        stage_info[stage['id']] = stage['name']
    return stage_info

