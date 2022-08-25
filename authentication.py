from googleapiclient import discovery
from google.oauth2 import service_account
SERVICE_ACCOUNT_FILE = "creds.json"


class ApiGoogle:

    def __init__(self, spreadsheet_id, range_=None):
        self.spreadsheet_id = spreadsheet_id
        self.range = range_
        creds = service_account.Credentials.from_service_account_file(filename=SERVICE_ACCOUNT_FILE)
        self.service = discovery.build('sheets', 'v4', credentials=creds)

    def read_data_ranges(self, options="FORMATTED_VALUE"):
        request = self.service.spreadsheets().values().batchGet(spreadsheetId=self.spreadsheet_id,
                                                                ranges=self.range,
                                                                valueRenderOption=options)
        response = request.execute()
        return response

    def update_data(self, body_values):
        result = self.service.spreadsheets().values().update(spreadsheetId=self.spreadsheet_id,
                                                             range=self.range,
                                                             valueInputOption="USER_ENTERED",
                                                             body=body_values)
        response = result.execute()
        return response

    def copy_paste(self, body):
        request = self.service.spreadsheets().batchUpdate(spreadsheetId=self.spreadsheet_id, body=body)
        response = request.execute()
        return response

    def add_1_row_end(self, sheet_id):
        body = {"requests": [{"appendDimension": {"sheetId": sheet_id, "dimension": "ROWS", "length": 1}}]}
        result = self.service.spreadsheets().batchUpdate(spreadsheetId=self.spreadsheet_id, body=body)
        response = result.execute()
        return response
