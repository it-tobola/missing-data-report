import notion_df as nd
import pandas as pd
import datetime
nd.pandas()

notion_token = "secret_omL8nzIdOZySUeAtSOCHm0bUNh2ydXdohuePKPBXkxm"
ee_db_id = "03764697bdf74f2b938313815cf62069"
id_url = "https://www.notion.so/03764697bdf74f2b938313815cf62069?v=e856d446c1a44cfcb8857b014f591284"

ee_df = pd.DataFrame(nd.download(ee_db_id, api_key=notion_token))

ee_df_trimmed = ee_df[["EE Code",
               'First Name',
               'Last Name',
               'Hire Date',
               'Shift',
               'Rotation',
               'Position Seat',
               'Direct Supervisor',
               'Home Base',
               'Evaluation Due Date',
               'Termination Date',
               'Status']]

def start(ee_path, save_path, savedate):

    # Create a dataframe from the excel file
    ee = pd.DataFrame(pd.read_csv(fr"{ee_path}"))

    ee.to_csv(fr"{save_path}\EE({savedate}).csv")

    ee['EE Code'] = ee['Employee_Code']
    ee['First Name'] = ee['Legal_Firstname']
    ee['Last Name'] = ee['Legal_Lastname']
    ee['Hire Date'] = ee['Hire_Date']
    ee['Position Seat'] = ee['Position_Seat_Number']
    ee['Termination Date'] = ee['Termination_Date']

    notion_ee = ee_df_trimmed


    return(ee, notion_ee)


def write_to_table(DataFrame, savepath):
    import azure_cnxn as az
    from sqlalchemy import create_engine
    # The next steps are used to drop the previous tables from the TOBOLA server
    #   and then create a replacement from the new data pull

    old = ee_df_trimmed
    new = DataFrame

    ##  Create Table
    from sqlalchemy.engine import URL
    cnxn_url = URL.create("mssql+pyodbc", query={"odbc_connect": az.cnxn_string})
    engine = create_engine(cnxn_url)
    old.to_sql("OLD", engine, index=False, if_exists='replace')

    ##  Create Table
    import sqlalchemy as sql
    from sqlalchemy.engine import URL
    cnxn_url = URL.create("mssql+pyodbc", query={"odbc_connect": az.cnxn_string})
    engine = create_engine(cnxn_url)
    new.to_sql("NEW", engine, index=False, if_exists='replace')

    cnxn_url = URL.create("mssql+pyodbc", query={"odbc_connect": az.cnxn_string})
    engine = sql.create_engine(cnxn_url)

    ee_update_query = """Select
    n.Employee_Code as 'EE Code',
    n.Legal_Firstname as 'First Name',
    n.Legal_Lastname as 'Last Name',
    n.Hire_Date as 'Hire Date',
    CASE 
        WHEN
            n.Termination_Date = '00/00/0000'
        THEN NULL
        ELSE n.Termination_Date
    END as 'Termination Date'
FROM
    NEW n 
RIGHT JOIN
    OLD o 
    ON
        o.[EE Code] = n.Employee_Code
WHERE
    o.[EE Code] is NULL and n.Legal_Firstname not like 'Test'"""
    updates = pd.read_sql_query(ee_update_query, con=engine)
    print("New Employees to NOTION")
    print(updates)

    # Add new employees to notion ee database
    updates.to_notion(id_url, title="Tests", api_key=notion_token)

    # List any discrepancies between employee information in new vs old
    def catch_discrepancies(older, newer):
        # Create an empty dataframe to store discrepancies
        discrepancies = pd.DataFrame(columns=older.columns)

        # Loop through each row of the old dataframe
        for i, old_row in older.iterrows():
            # Find the corresponding row in the new dataframe
            new_row = newer[newer['EE Code'] == old_row['EE Code']]

            # Check if the new_row is empty (i.e. the corresponding row was not found in the new dataframe)
            if new_row.empty:
                # If the corresponding row was not found, add the old row to the discrepancies dataframe
                discrepancies = discrepancies.append(old_row, ignore_index=True)
            else:
                # Compare each column in the old row to the corresponding column in the new row
                for col in older.columns:
                    if old_row[col] != new_row.iloc[0][col]:
                        # If the values are different, add the old row to the discrepancies dataframe
                        discrepancies = discrepancies.append(old_row, ignore_index=True)
                        break

        return discrepancies

    # Call the function to get the discrepancies dataframe
    discrepancies = catch_discrepancies(old, new)

    # Print the discrepancies dataframe
    print(discrepancies)

    discrepancies.to_excel(fr"{savepath}\HR\ee_discrepancies.xlsx")

def training_report(date):
    today = datetime.datetime.now().date()

    report = pd.DataFrame()
    report["EE Code"] = ee_df["EE Code"]
    report["Name"] = ee_df['First Name'] + " " + ee_df['Last Name']
    report["LLAM"] = ee_df["LLAM Date"]
    report["Med Pass Completion"] = ee_df["Med Pass Completion Date"]
    report["LLAM Due Date"] = ee_df["LLAM Due Date"]
    report["MANDT"] = ee_df["MANDT Date"]
    report["MANDT Due Date"] = ee_df["MANDT Due Date"]
    report["CPR"] = ee_df["CPR Date"]
    report["CPR Due Date"] = ee_df["CPR Due Date"]
    report["DL Expiration"] = ee_df["DL Expiration Date"]
    report["Position"] = ee_df["Position"]
    report["Department"] = ee_df["Home Base"]
    report["Status"] = ee_df["Status"]
    report = pd.DataFrame(report.loc[report['Status'] == "Active"])

    report["Compliance"] = (report["LLAM Due Date"].dt.date <= today) | \
                           (report["MANDT Due Date"].dt.date <= today) | \
                           (report["CPR Due Date"].dt.date <= today) | \
                           (report["DL Expiration"].dt.date <= today)


    # Save location for the report
    save = fr"C:/Users/olato/OneDrive/Desktop/TOBOLA QA REVIEW/REPORTS/training_reports"
    dsp = pd.DataFrame(report.loc[report['Position'] == "DSP"])
    dsp.to_excel(fr"{save}/training_report({date}).xlsx")

    return(dsp)