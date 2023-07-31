import pandas as pd
import notion_df as nd
import notion_db
import datetime
nd.pandas()


# client attendance
def atn(data):
    atn_file = fr"C:\Users\olato\OneDrive\Desktop\TOBOLA QA REVIEW\REPORTS\Viz_Data_Source\SR_Attendance.xlsx"
    report = pd.read_excel(atn_file).append(data)
    report = report[["program_site",
                      "individual",
                      "MCI",
                      "date",
                      "attendance"]]
    report = report.drop_duplicates()
    report.to_excel(atn_file, index=False)
    print("ATN Report Completed")


def program_spending():
    tx_file = fr"C:\Users\olato\OneDrive\Desktop\TOBOLA QA REVIEW\REPORTS\Viz_Data_Source\Program_tx.xlsx"
    tx_id = notion_db.transactions
    tx = pd.read_notion(tx_id, api_key=notion_db.key, resolve_relation_values=True)
    tx = tx[tx.Verify == True]
    report = pd.read_excel(tx_file).append(pd.DataFrame(tx)).reset_index(drop=True)
    subset_cols = ["TxID"]
    report = report.drop_duplicates(subset=subset_cols, keep='last').reset_index(drop=True)
    report.to_excel(tx_file, index=False)
    print("Program Spending Report Completed")


def overtime(data):
    tc = data.drop(columns=[
        'HomeDepartment',
        'Pay Class',
        'Badge',
        'Dollars',
        'Employee Approved',
        'Supervisor Approved',
        'Tax Profile',
        'Home Department Desc',
        'Distributed Department Code'])

    tc1 = tc[tc["EarnCode"].isnull()]
    tc2 = tc[tc["EarnCode"] == "R"]
    tc = tc1.append(tc2)

    # Datatype Corrections
    try:
        tc['InPunchDay'] = tc['InPunchDay'].replace(['0000-00-00', None])
        tc['InPunchDay'] = pd.to_datetime(tc['InPunchDay']).dt.date
    except:
        print("failed to convert InPunchDay")
        pass
    try:
        tc['InPunchTime'] = pd.to_datetime(tc['InPunchTime'])
    except:
        print("failed to convert InPunchTime")
        pass
    try:
        tc['OutPunchDay'] = tc['OutPunchDay'].replace(['0000-00-00', None])
        tc['OutPunchDay'] = pd.to_datetime(tc['OutPunchDay']).dt.date
    except:
        print("failed to convert OutPunchDay")
        pass
    try:
        tc['OutPunchTime'] = pd.to_datetime(tc['OutPunchTime'])
    except:
        print("failed to convert OutPunchTime")
        pass

    tc['Week'] = pd.to_datetime(tc['InPunchDay']).dt.week
    tc['Month'] = pd.to_datetime(tc['InPunchDay']).dt.month
    tc['Year'] = pd.to_datetime(tc['InPunchDay']).dt.year

    ot_report = tc.groupby(['EECode',
                            'Firstname',
                            'Lastname',
                            'Department',
                            'Week',
                            'Month',
                            'Year'])['EarnHours'].aggregate('sum').reset_index()
    ot_report = ot_report[ot_report.EarnHours > 40].reset_index(drop=True)
    ot_report["OT"] = ot_report.EarnHours - 40

    ot_file = fr"C:\Users\olato\OneDrive\Desktop\TOBOLA QA REVIEW\REPORTS\Viz_Data_Source\OT_Report.xlsx"
    report = pd.read_excel(ot_file).append(ot_report)
    report = report.drop_duplicates()
    report.to_excel(ot_file, index=False)
    print("Overtime Report Completed")


def utilization(data):
    utz_file = fr"C:\Users\olato\OneDrive\Desktop\TOBOLA QA REVIEW\REPORTS\Viz_Data_Source\SH_Utilization.xlsx"
    report = pd.read_excel(utz_file).append(data)
    report = report[["Date",
                     "Department",
                     "SH",
                     "Earn Hours",
                     "UTZ"]]
    report = report.drop_duplicates()
    report.to_excel(utz_file, index=False)
    print("UTZ Report Completed")


def training():
    training_file = fr"C:\Users\olato\OneDrive\Desktop\TOBOLA QA REVIEW\REPORTS\Viz_Data_Source\ee_training.xlsx"
    positions_id = notion_db.positions
    positions = pd.read_notion(positions_id, api_key=notion_db.key, resolve_relation_values=True)
    ee_id = notion_db.ee
    ee = pd.read_notion(ee_id, api_key=notion_db.key, resolve_relation_values=True)
    ee = ee[ee["Status"] != "Terminated"]
    wl_id = notion_db.work_locations
    wl = pd.read_notion(wl_id, api_key=notion_db.key, resolve_relation_values=True)
    today = datetime.datetime.now().date()


    report_columns = ["EE Code",
                      "Full Name",
                      "Department",
                      "Work Location",
                      "BCC Date",
                      "Drug Test",
                      "DL Expiration Date",
                      "PPD Step 1",
                      "PPD Step 2",
                      "(TB) Blood Testing",
                      "Chest X-Ray",
                      "Physical",
                      "PM5",
                      "PM46",
                      "Initial Orientation",
                      "Title 16",
                      "Guiding Principles",
                      "Basic Driving",
                      "Site Specific Orientation",
                      "CPR Due Date",
                      "MANDT Due Date",
                      "LLAM Due Date"]

    report = pd.DataFrame(columns=report_columns)

    report["EE Code"] = ee["EE Code"]
    report["Full Name"] = ee["Full Name"]

    # department
    for i, row in report.iterrows():
        eec = row["EE Code"]
        for r, bow in positions.iterrows():
            dept = bow["Department"]
            location = bow["Work Location"]
            if eec in bow["EE Code"]:
                row["Department"] = dept
                row["Work Location"] = location

    # work location
    for i, row in report.iterrows():
        eec = row["EE Code"]
        location = format(row["Work Location"])[2:-2]
        for r, bow in report.iterrows():
            if eec == bow["EE Code"]:
                row["Work Location"] = location

    # trainings
    report = report.append(ee)

    report = pd.DataFrame(data=report, columns=report_columns)





    print(report.columns)
    print(report)


