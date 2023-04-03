import pandas as pd
import azure_cnxn as az
from sqlalchemy import create_engine

def start(timecard_path, save_path, savedate):

    # Create a dataframe from the excel file
    tc = pd.DataFrame(pd.read_csv(fr"{timecard_path}"))

    # Seperate the time punch columns into 2 seperate entities (date, time)
    ## InPunch
    in_dates = pd.DataFrame(tc.InPunchTime.str.split(" ", expand=True, n=1)[0])
    in_time = pd.DataFrame(tc.InPunchTime.str.split(" ", expand=True, n=1)[1])
    tc['InPunchDay'] = in_dates
    tc['InPunchTime'] = in_time

    ## OutPunch
    out_dates = pd.DataFrame(tc.OutPunchTime.str.split(" ", expand = True, n=1)[0])
    out_time = pd.DataFrame(tc.OutPunchTime.str.split(" ", expand = True, n=1)[1])
    tc['OutPunchDay'] = out_dates
    tc['OutPunchTime'] = out_time


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

    tc.to_csv(fr"{save_path}\TimeCards({savedate}).csv")

    return(tc)

    # Once saved, remember to go into the csv file and null out any OutDays == '0000-00-00'
    #   and any OutTimes == '12:00:00 AM'. These are MISSING out dates/times that are being recorded as 0

def write_to_table(DataFrame):
    # The next steps are used to drop the previous tables from the TOBOLA server
    #   and then create a replatement from the new data pull

    tc = DataFrame

    ##  Create Table
    from sqlalchemy.engine import URL
    cnxn_url = URL.create("mssql+pyodbc", query={"odbc_connect": az.cnxn_string})
    engine = create_engine(cnxn_url)
    tc.to_sql("TimeCards2022", engine, index=False, if_exists='replace')

    return(tc)


def staff_ot_report(timecards):
    report = timecards[['EECode', 'InPunchDay', 'EarnHours']]
    report['Year'] = pd.to_datetime(report['InPunchDay']).dt.year
    report['Week'] = pd.to_datetime(report['InPunchDay']).dt.week
    report = report[['EECode', 'EarnHours', 'Year', 'Week']]

    # Group by employee, year, and week and calculate the sum of earned hours for each group
    week_totals = report.groupby(['EECode', 'Year', 'Week'])['EarnHours'].sum().reset_index()

    # Merge the week_totals dataframe back into the report dataframe
    report = pd.merge(report, week_totals, on=['EECode', 'Year', 'Week'], suffixes=('', '_week_total'))

    report = report[['EECode', 'Year', 'Week', 'EarnHours_week_total']]
    report = report.drop_duplicates()

    report['Overtime'] = report['EarnHours_week_total'] - 40

    report = report[report["Overtime"]>0]
    report = report.reset_index()
    report = report[['EECode', 'Year', 'Week', 'EarnHours_week_total', 'Overtime']]

    print("OT REPORT")
    print(report)

    return(report)





