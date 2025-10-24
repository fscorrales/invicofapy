#!/usr/bin/env python3
"""
Author: Fernando Corrales <fscpython@gmail.com>
Purpose: Upload DataFrame to Google Sheets
Source:
    - https://towardsdatascience.com/using-python-to-push-your-pandas-dataframe-to-google-sheets-de69422508f
    - https://medium.com/@jb.ranchana/write-and-append-dataframes-to-google-sheets-in-python-f62479460cf0
    - https://medium.com/@vince.shields913/reading-google-sheets-into-a-pandas-dataframe-with-gspread-and-oauth2-375b932be7bf
"""

__all__ = ["GoogleSheets"]


import argparse
import json
from dataclasses import dataclass, field
from typing import Optional

import gspread  # https://docs.gspread.org/en/latest/index.html#
import pandas as pd
from google.oauth2.service_account import Credentials

from ..config import settings

# from google.oauth2.service_account import Credentials


# --------------------------------------------------
@dataclass
class GoogleSheets:
    """Upload DataFrame to Google Sheets
    :param path_credentials_file: json file download from Google
    """

    # path_credentials_file:str
    # credentials: ServiceAccountCredentials = field(
    #     default=None, init=False, repr=False
    # )
    path_credentials_file: Optional[str] = None
    credentials: Credentials = field(default=None, init=False, repr=False)
    gc: gspread.Client = field(default=None, init=False, repr=False)

    # --------------------------------------------------
    def __post_init__(self):
        self.authorize_access()

    # --------------------------------------------------
    def authorize_access(self):
        scopes = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]

        credentials_json = settings.GOOGLE_CREDENTIALS
        if credentials_json:
            try:
                credentials_dict = json.loads(credentials_json)
                self.credentials = Credentials.from_service_account_info(
                    credentials_dict, scopes=scopes
                )
            except Exception as e:
                raise ValueError("Invalid GOOGLE_CREDENTIALS format") from e
        elif self.path_credentials_file:
            self.credentials = Credentials.from_service_account_file(
                self.path_credentials_file, scopes=scopes
            )
        else:
            raise ValueError("No valid credentials provided")
        # self.credentials = ServiceAccountCredentials.from_json_keyfile_name(
        #     self.path_credentials_file, scope
        # )
        self.gc = gspread.authorize(self.credentials)

    # --------------------------------------------------
    def to_google_sheets(
        self, df: pd.DataFrame, spreadsheet_key: str, wks_name: str = "Hoja 1"
    ):
        """Method to upload DataFrame to Google Sheets
        :param df: pandas DataFrame to upload
        :param spreadsheet_key: can be found in the URL of a
        previously created sheet
        :param wks_name: Worksheet name
        """
        try:
            sheet = self.gc.open_by_key(spreadsheet_key)
            try:
                worksheet = sheet.worksheet(wks_name)
            except gspread.exceptions.WorksheetNotFound:
                worksheet = sheet.add_worksheet(title=wks_name, rows="100", cols="20")
            if df.empty:
                delete_range = f"{wks_name}!A2:ZZ100000"
                sheet.values_clear(delete_range)
                print(f"❌ Data deleted from '{wks_name}' because DF was empty")
            else:
                worksheet.clear()
                worksheet.update([df.columns.values.tolist()] + df.values.tolist())
                print(f"✅ Data uploaded successfully to '{wks_name}'")
        except Exception as e:
            print(f"❌ Error uploading data: {e}")
            raise

    # https://github.com/maybelinot/df2gspread/issues/41#issuecomment-1154527949

    # def upload_pandas_df(self, df:pd.DataFrame):
    #   values = [df.columns.values.tolist()]
    #   values.extend(df.values.tolist())
    #   sheet.values_update(
    #     self.title,
    #     params = { 'valueInputOption': 'USER_ENTERED' },
    #     body = { 'values': values }
    #   )

    # gspread.Worksheet.upload_pandas_df = upload_pandas_df


# --------------------------------------------------
def get_args():
    """Get needed params from user input"""
    parser = argparse.ArgumentParser(
        description="Upload DataFrame to Google Sheets",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "spreadsheet_key",
        metavar="spreadsheet_key",
        type=str,
        help="can be found in the URL of a previously created sheet",
    )

    parser.add_argument(
        "-c",
        "--credentials",
        metavar="json_credentials",
        type=str,
        help="Google's json file credentials name. Must be in the same folder",
        default=None,
    )

    parser.add_argument(
        "-w",
        "--wks_name",
        metavar="worksheet_name",
        default="Hoja 1",
        type=str,
        help="worksheet_name",
    )

    return parser.parse_args()


# --------------------------------------------------
def main():
    """Let's try it"""
    d = {"col1": [1, 2], "col2": [3, 4]}
    df = pd.DataFrame(data=d)

    args = get_args()

    gs = GoogleSheets(args.credentials)
    gs.to_google_sheets(
        df, spreadsheet_key=args.spreadsheet_key, wks_name=args.wks_name
    )
    print("Upload complete")


# --------------------------------------------------
if __name__ == "__main__":
    main()
