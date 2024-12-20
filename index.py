# app.py  
from flask import Flask, request, jsonify, render_template, send_file  
import time  
import threading  
import os  
import requests
import pandas as pd 

app = Flask(__name__)  

total_result = []

# Global variable to store task status  
task_status = {"progress": 0, "message": "", "completed": False}  

# Define the URL
url = 'https://www.listreports.com/v1/babou/f/searchAgentsV2'  # Replace with your URL

# Define the headers
headers = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Accept-Language': 'en-US,en;q=0.9,ru;q=0.8',
    'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6InJ5MTA4YTR4TCIsImlhdCI6MTczNDI5NzA0OCwiZXhwIjoxNzM2ODg5MDQ4fQ.J9xDq2LOAOXCJApAczApJum4Of2mKay28YFbOv3xSbc',
    'Content-Type': 'application/json;charset=UTF-8',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
}

# Define the payload

def set_limit(new_limit, payload):  
    payload["args"][2]["limit"] = new_limit  
    return payload

def add_zipcode_to_payload(zipcodes, payload):  
    for zipcode in zipcodes:
        new_term = {  
            "formatted_term": zipcode,  
            "id": zipcode,  
            "search_term": zipcode,  
            "search_type": "zipcode"  
        }  
        payload["args"][1].append(new_term)  

    return payload

def export_excel(full_names, emails, phones, total_sales, total_volumes, median_sales_prices):
    df1 = pd.DataFrame(full_names, columns=['Name'])  
    df2 = pd.DataFrame(emails, columns=['Email'])  
    df3 = pd.DataFrame(phones, columns=['Phone'])  
    df4 = pd.DataFrame(total_sales, columns=['Total Sale'])  
    df5 = pd.DataFrame(total_volumes, columns=['Total Volume'])  
    df6 = pd.DataFrame(median_sales_prices, columns=['Median Sales Price'])  

    # Concatenate DataFrames along the columns  
    combined_df = pd.concat([df1, df2, df3, df4, df5, df6], axis=1)  

    # Create a Pandas Excel writer using 'openpyxl' as the engine  
    with pd.ExcelWriter('result.xlsx', engine='openpyxl') as writer:  
        combined_df.to_excel(writer, sheet_name='Combined Arrays', index=False)  

    print("Excel file has been created successfully!")

def format_value(value):  
    if value == 0:  
        return "0"  
    elif value >= 1_000_000:  
        return f"{value / 1_000_000:.0f}M"  # Format value in millions  
    elif value >= 1_000:  
        return f"{value / 1_000:.0f}K"  # Format value in thousands  
    else:  
        return str(value)  # Just return the value as a string  
    

def export_excel_file(zip_code_list):
    global total_result
    payload = {
    "args": [
        "either",
        [
            # {
            #     # "formatted_term": "18966",
            #     # "id": "18966",
            #     # "search_term": "18966",
            #     # "search_type": "zipcode"
            # }
        ],
        {
            "cache": True,
            "skip": 0,
            "limit": 1,
            "share_wallet_for": "50284",
            "format": "json",
            "user_status": "either",
            "paired_status": "either",
            "done_business_with": False,
            "follow_status": None,
            "orderBy": "SHARED_TRANSACTIONS_PERCENT_LAST_12_MONTHS:desc",
            "is_following": "ry108a4xL",
            "replace_with_portal_details": True
        }
    ]
}
    
    payload = add_zipcode_to_payload(zip_code_list, payload)

    response = requests.post(url, headers=headers, json=payload)

    # Check the response
    if response.status_code == 200 or response.status_code == 201:
        try:
            # Parse the response JSON
            data = response.json()
            
            total_limit = data.get('data').get('data').get('total')
            payload = set_limit(total_limit, payload)
            final_response = requests.post(url, headers=headers, json=payload)

            if response.status_code == 200 or response.status_code == 201:
                print("Data received successfully.")
                try:
                    data = final_response.json()
                    # Extract 'VANDER_ID' values
                    vander_ids = []
                    # Extract VANDER_IDs from agents
                    vander_ids = [agent["VANDER_ID"] for agent in data.get("data", {}).get("data", {}).get("agents", []) if "VANDER_ID" in agent]
                    first_names = [agent["AGENT_FIRST_NAME"] for agent in data.get("data", {}).get("data", {}).get("agents", []) if "AGENT_FIRST_NAME" in agent]
                    last_names = [agent["AGENT_LAST_NAME"] for agent in data.get("data", {}).get("data", {}).get("agents", []) if "AGENT_LAST_NAME" in agent]
                    full_names = []
                    for index in range(len(first_names)):  
                        full_names.append(first_names[index] + ' ' + last_names[index])
                    phones = [agent["AGENT_PHONE"] for agent in data.get("data", {}).get("data", {}).get("agents", []) if "AGENT_PHONE" in agent]
                    total_sales = [agent["BUYSIDES_LAST_12_MONTHS"] for agent in data.get("data", {}).get("data", {}).get("agents", []) if "BUYSIDES_LAST_12_MONTHS" in agent]
                    total_volumes_original = [agent["BUYSIDE_DOLLAR_AMOUNT_LAST_12_MONTHS"] for agent in data.get("data", {}).get("data", {}).get("agents", []) if "BUYSIDE_DOLLAR_AMOUNT_LAST_12_MONTHS" in agent]
                    total_volumes = [format_value(value) for value in total_volumes_original]
                    median_sales_prices_original = [agent["BUYSIDE_MEDIAN_PRICE_LAST_12_MONTHS"] for agent in data.get("data", {}).get("data", {}).get("agents", []) if "BUYSIDE_MEDIAN_PRICE_LAST_12_MONTHS" in agent]
                    median_sales_prices = [format_value(value) for value in median_sales_prices_original]
                    agent_emails = [agent["AGENT_EMAILS"] for agent in data.get("data", {}).get("data", {}).get("agents", []) if "AGENT_EMAILS" in agent]
                    emails = []
                    for agent_email in agent_emails:
                        if agent_email:
                            emails.append(agent_email[0])
                        else: 
                            emails.append('None')
                    
                    export_excel(full_names, emails, phones, total_sales, total_volumes, median_sales_prices)
                    
                    total_result = {  
                        "full_names": full_names,  
                        "emails": emails,  
                        "phones": phones,  
                        "total_sales": total_sales,  
                        "total_volumes": total_volumes,  
                        "median_sales_prices": median_sales_prices  
                    }
                except ValueError as e:
                    print("Failed to parse JSON response:", e)
        except ValueError as e:
            print("Failed to parse JSON response:", e)
    else:
        print(f"Error: {response.status_code} - {response.text}")

def long_running_task(zip_codes):  
    # Simulate a long-running task  
    export_excel_file(zip_codes)  # Replace this with actual processing logic  

def get_result(zip_codes):  
    global task_status  
    global total_result
    task_status = {"progress": 0, "message": "", "completed": False}
    task_status["progress"] = 0  
    task_status["message"] = "Processing started..."  

    # Simulate a long-running task  
    # threading.Thread(target=long_running_task, args=(zip_codes,), daemon=True).start()
    result = long_running_task(zip_codes)

    task_status["message"] = "Processing completed!"  
    task_status["progress"] = 100  
    task_status["completed"] = True  # Mark the task as completed    

@app.route('/')  
def index():  
    return render_template('index.html')  

@app.route('/process_zip_codes', methods=['POST'])  
def process_zip_codes():  
    zip_codes = request.json.get('zip_codes', '')  
    zip_codes_list = [zip_code for zip_code in zip_codes.split() if zip_code]  

    # Start the long-running task in a separate thread  
    threading.Thread(target=get_result, args=(zip_codes_list,), daemon=True).start()  

@app.route('/task_status', methods=['GET'])  
def task_status_route():  
    return jsonify(task_status) 
 
@app.route('/get_total_result', methods=['GET'])  
def get_total_result_route():  
    return jsonify(total_result)  

@app.route('/download', methods=['GET'])  
def download_file():  
    if task_status["completed"]:  
        return send_file("result.xlsx", as_attachment=True)  
    return jsonify({"message": "File not ready yet."})  

if __name__ == '__main__':  
    app.run(debug=False)
