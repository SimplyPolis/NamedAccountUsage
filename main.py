import logging
import os.path
from collections import defaultdict
from io import BytesIO

import backoff as backoff
import requests
import requests.exceptions
import re
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import datetime
import sys
import pandas as pd


argslen = len(sys.argv) #Handle args
practiceblockfolder="1cQx3hBZcHFqwE47CbHtciKu4ZWDzyWMw"
Player='FuchsFrikadelle'
#date=datetime.datetime.fromisoformat("2023-03-21 17:46:15")
date=datetime.datetime.now()
reg="\d{4}-\d{2}-\d{2}\s+\[\w+\]"
#try:
  #  Outfit=sys.argv[1]
   # if (argslen==3):
   #     HowFar=int(sys.argv[2])
#except:
 #   print("You messed up the args")
 #  exit(1)

RANGE_NAME = "Sheet1!D7:D"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly','https://www.googleapis.com/auth/drive']

@backoff.on_predicate(
    backoff.expo,
    predicate=lambda r: r.status_code == 429,
    jitter=backoff.full_jitter,
)

def get_csv(link,creds):
   return requests.get(link, headers={"Authorization": "Bearer " + creds.token})

def handle_block(driveservice,file,datetocomp,creds):
    rev_page_token = None
    revstotest = []
    while True:
        revisions = driveservice.revisions().list(fileId=file.get('id'),
                                                  fields='nextPageToken, revisions(modifiedTime, exportLinks)',
                                                  pageToken=rev_page_token, pageSize=1000).execute()

        for rev in revisions.get('revisions', []):
            date = datetime.datetime.fromisoformat(rev.get("modifiedTime")[0:-5])
            if (datetocomp <= date):
                revstotest.append((rev.get('exportLinks').get("text/csv"), date))
        rev_page_token = revisions.get('nextPageToken', None)
        if rev_page_token is None:
            break
    revstotest.sort(key=lambda l: l[1])
    data = get_csv(revstotest[0][0], creds)
    dfi = pd.read_csv(BytesIO(data.content), dtype=str, keep_default_na=False)
    data = get_csv(revstotest[-1][0], creds)
    dff = pd.read_csv(BytesIO(data.content), dtype=str, keep_default_na=False)
    dfc = pd.concat([dfi, dff]).drop_duplicates(keep=False)
    for (columnName, columnData) in dfc.items():
        if (columnData.str.contains(Player, case=False, regex=False).any()):
            print(columnName)



def main(datetotest):
        creds = None
        if os.path.exists('token.json'):  # Try to use a token
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                # If it can not refresh grab from credentials file and web browser auth
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.json', 'w') as token:
                token.write(creds.to_json())
        try:
            # create drive api client
            driveservice = build('drive', 'v3', credentials=creds)  # services
            sheetsservice = build('sheets', 'v4', credentials=creds)
            drive_page_token = None

            datetocomp = (datetotest - datetime.timedelta(weeks=8))  # date calculations
            datetext = datetocomp.replace(microsecond=0).isoformat()
            while True:

                # pylint: disable=maybe-no-member
                response = driveservice.files().list(
                    q="fullText contains '{}' and modifiedTime > '{}' and mimeType = 'application/vnd.google-apps.spreadsheet'".format(
                        Player, datetext),  # drive quarry
                    spaces='drive',
                    fields='nextPageToken, '
                           'files(id, name, parents)',
                    orderBy='modifiedTime desc',
                    pageToken=drive_page_token).execute()
                for file in response.get('files', []):  # Go through files
                    if(file.get("parents")[0]=="1cQx3hBZcHFqwE47CbHtciKu4ZWDzyWMw"):
                        print(file.get("name"))
                        handle_block(driveservice,file,datetocomp,creds)
                    else:
                        if(re.search(reg,file.get("name"))):
                            print(file.get("name"))
                        else:
                            print("idk what this is {}".format(file.get("name")))
                drive_page_token = response.get('nextPageToken', None)
                if drive_page_token is None:
                    break

        except HttpError as error:  # something bad has happend
            print(F'An error occurred: {error}')
            files = None

main(date)