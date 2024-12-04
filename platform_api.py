from fastapi import FastAPI, HTTPException, Request , Query
from pydantic import BaseModel
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime
from typing import List, Union
import json

app = FastAPI()

# Google Sheets API configuration
SERVICE_ACCOUNT_FILE = '/etc/secrets/gsheetcreds.json'
SPREADSHEET_ID = '10mXRRhQ0MWphsSTdCKgBFOpQYIUZd5wwSCfHRxsc9Lo'
RANGE_NAME = 'sensor_data!A2:E'  # Adjusted to allow dynamic column range

# Google Sheets API credentials
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)

service = build('sheets', 'v4', credentials=credentials)
sheet = service.spreadsheets()

# Define the data model for the incoming JSON structure
class SensorDataIncoming(BaseModel):
    cmd: str
    seqno: int = None
    EUI: str
    ts: int = None
    fcnt: int = None
    data: str
    rssi: float = None
    snr: float = None
    protocol: str

# Define the data model for Google Sheets (subset of SensorDataIncoming)
class SensorData(BaseModel):
    EUI: str
    ts: int
    data: str

@app.post("/Sensor-data/")
async def post_data(data: Union[List[SensorDataIncoming], SensorDataIncoming]):
    try:
        # Convert single object input to a list
        if isinstance(data, SensorDataIncoming):
            data = [data]
        
        # Prepare data to insert into Google Sheets
        values = []
        jsonData = []
        for entry in data:
            # Convert ts to a readable timestamp in UTC
            timestamp = datetime.utcfromtimestamp(entry.ts / 1000).strftime("%m-%d-%Y %H:%M:%S") if entry.ts else ""
            sensor_data = SensorData(EUI=entry.EUI, ts=entry.ts or 0, data=entry.data)
            input_data = {
              "bytes": [int(sensor_data.data[i:i+2], 16) for i in range(0, len(sensor_data.data), 2)],
               "fPort": 1  # Adjust according to the protocol
            }
            data = decode_uplink(input_data)
            jsonData.append({
              "EUI": sensor_data.EUI,
              "Timestamp": sensor_data.ts,
              "Formatted Timestamp": timestamp,
              "Data": sensor_data.data,
              "Decoded Data": data
            })
            values.append([sensor_data.EUI, sensor_data.ts, sensor_data.data, timestamp, json.dumps(jsonData)])

        # Prepare body for API request
        body = {'values': values}

        # Append data to Google Sheets
        sheet.values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_NAME,
            valueInputOption="RAW",
            body=body
        ).execute()

        return {"status": "Data successfully added to Google Sheet", "code": 201}
    except Exception as e:
        # Raise a 500 error with detailed information
        raise HTTPException(status_code=500, detail=f"Failed to add data: {str(e)}")
        
@app.get("/Sensor-data/")
async def get_data():
    try:
        # Fetch data from Google Sheets
        result = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_NAME
        ).execute()
        
        values = result.get('values', [])
        
        # Extract and parse JSON strings from column D (index 3)
        json_data_list = []
        for row in values:
            if len(row) > 4:  # Ensure there are enough columns
                json_string = row[4]  # Assuming column D is the fifth column (index 4)
                try:
                    json_data = json.loads(json_string)  # Parse the JSON string
                    json_data_list.append(json_data)
                except json.JSONDecodeError:
                    continue  # Skip rows with invalid JSON

        return {"status": "Data fetched successfully", "data": json_data_list}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch data: {str(e)}")
        
@app.get("/Sensor-data/Sensor/")
async def get_data(EUI:str, last: int = Query(1, ge=1, le=1000)):
    try:
        # Fetch data from Google Sheets
        result = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_NAME
        ).execute()
        
        values = result.get('values', [])

        # Initialize the list for filtered data
        filtered_data = []

        for row in values:
            if len(row) > 4:  # Ensure there are enough columns
                row_eui = row[0]  # Assuming EUI is in the first column
                json_string = row[4]  # Assuming JSON is in the fifth column
                if row_eui == EUI:  # Filter by EUI
                    try:
                        json_data = json.loads(json_string)  # Parse the JSON string
                        filtered_data.append({
                            "EUI": row_eui,
                            "Timestamp": row[1],  # Assuming Timestamp is in the second column
                            "Data": row[2],       # Assuming Data is in the third column
                            "Updated_at": row[3], # Assuming Updated_at is in the fourth column
                            "JSON": json_data
                        })
                    except json.JSONDecodeError:
                        continue  # Skip rows with invalid JSON
        
        # Sort by timestamp or updated_at if needed, then get the last 'n' entries
        filtered_data.sort(key=lambda x: x["Timestamp"], reverse=True)
        result_data = filtered_data[:last]

        return {"status": "Data fetched successfully", "data": result_data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch data: {str(e)}")    
    
@app.get("/Sensor-data/range/")
async def get_data_by_range(EUI: str, start: str, end: str):
    try:
        # Convert start and end date strings to timestamps
        start_timestamp = int(datetime.strptime(start, "%m-%d-%Y %H:%M:%S").timestamp() * 1000)
        end_timestamp = int(datetime.strptime(end, "%m-%d-%Y %H:%M:%S").timestamp() * 1000)

        # Fetch data from Google Sheets
        result = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_NAME
        ).execute()
        
        values = result.get('values', [])

        # Initialize the list for filtered data
        filtered_data = []

        for row in values:
            if len(row) > 4:  # Ensure there are enough columns
                row_eui = row[0]  # Assuming EUI is in the first column
                row_timestamp = int(row[1])  # Assuming Timestamp is in the second column
                json_string = row[4]  # Assuming JSON is in the fifth column
                if row_eui == EUI and start_timestamp <= row_timestamp <= end_timestamp:  # Filter by EUI and timestamp range
                    try:
                        json_data = json.loads(json_string)  # Parse the JSON string
                        filtered_data.append({
                            "EUI": row_eui,
                            "Timestamp": row[1],  # Assuming Timestamp is in the second column
                            "Data": row[2],       # Assuming Data is in the third column
                            "Updated_at": row[3], # Assuming Updated_at is in the fourth column
                            "JSON": json_data
                        })
                    except json.JSONDecodeError:
                        continue  # Skip rows with invalid JSON
        
        return {"status": "Data fetched successfully", "data": filtered_data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch data: {str(e)}")   
        

def decode_uplink(input):
    decoded = {}
    bytes = input['bytes']

    if input['fPort'] == 1:  # TELEMETRY

        # decode header
        decoded['base_id'] = bytes[0] >> 4
        decoded['major_version'] = bytes[0] & 0x0F
        decoded['minor_version'] = bytes[1] >> 4
        decoded['product_version'] = bytes[1] & 0x0F
        decoded['up_cnt'] = bytes[2]
        decoded['battery_voltage'] = ((bytes[3] << 8) | bytes[4]) / 1000.0
        decoded['internal_temperature'] = ((bytes[5] << 8) | bytes[6]) / 10 - 100
        decoded['networkBaseType'] = 'lorawan'
        decoded['networkSubType'] = 'tti'
        
        it = 7
        
        if decoded['minor_version'] >= 3:
            it = 7

            # Luftfeuchte ist bei allen Varianten enthalten
            decoded['humidity'] = bytes[it]
            it += 1

            if decoded['product_version'] & 0x01:  # Co2 und Druck sind enthalten wenn subversion bit0 = 1, andernfalls 0
                decoded['pressure'] = (bytes[it] << 8 | bytes[it + 1])
                it += 2
                decoded['co2_ppm'] = (bytes[it] << 8 | bytes[it + 1])
                it += 2
            else:
                it += 4  # Werte sind 0 aus kompatibilitäts Gründen, daher überspringen

            decoded['alarm'] = bytes[it]
            it += 1  # Alarm-Level, entspricht grün, gelb, rot

            # FIFO Werte wegwerfen (1 byte fifo size, 1 byte period, 7 bytes pro fifo eintrag)
            it += 2 + bytes[it] * 7

            decoded['dew_point'] = ((bytes[it] << 8) | bytes[it + 1]) / 10 - 100
            it += 2
            
            # Wandtemperatur und Feuchte enthalten wenn subversion bit 2 = 1
            if decoded['product_version'] & 0x04:
                decoded['wall_temperature'] = ((bytes[it] << 8) | bytes[it + 1]) / 10 - 100
                it += 2
                decoded['therm_temperature'] = ((bytes[it] << 8) | bytes[it + 1]) / 10 - 100
                it += 2
                decoded['wall_humidity'] = bytes[it]
                it += 1

        else:
            it = 7

            # Luftfeuchte ist bei allen Varianten enthalten
            decoded['humidity'] = bytes[it]
            it += 1

            if decoded['product_version'] & 0x01:  # Co2 und Druck sind enthalten wenn subversion bit0 = 1, andernfalls 0
                decoded['pressure'] = (bytes[it] << 8 | bytes[it + 1])
                it += 2
                decoded['co2_ppm'] = (bytes[it] << 8 | bytes[it + 1])
                it += 2
            else:
                it += 4  # Werte sind 0 aus kompatibilitäts Gründen, daher überspringen

            decoded['alarm'] = bytes[it]
            it += 1  # Alarm-Level, entspricht grün, gelb, rot

            # FIFO Werte wegwerfen (1 byte fifo size, 1 byte period, 7 bytes pro fifo eintrag)
            it += 2 + bytes[it] * 7

            # Taupunkt seit minor version 2 bei alle Varianten enthalten (ausnahme früher versionen subversion 2, daher byte prüfen)
            if decoded['minor_version'] >= 2 and bytes[it]:
                decoded['dew_point'] = bytes[it] - 100
                it += 1

            # Wandtemperatur und Feuchte enthalten wenn subversion bit 2 = 1
            if decoded['product_version'] & 0x04:
                decoded['wall_temperature'] = bytes[it] - 100
                it += 1
                decoded['therm_temperature'] = bytes[it] - 100
                it += 1
                decoded['wall_humidity'] = bytes[it]
                it += 1

    return {
        'data': decoded,
        'warnings': [],
        'errors': []
    }
