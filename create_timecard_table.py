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
    report = timecards[['EECode', 'Firstname', 'InPunchDay', 'EarnHours']]
    report['Year'] = pd.to_datetime(report['InPunchDay']).dt.year
    report['Week'] = pd.to_datetime(report['InPunchDay']).dt.week
    report = report[['EECode', 'Firstname', 'EarnHours', 'Year', 'Week']]

    # Group by employee, year, and week and calculate the sum of earned hours for each group
    week_totals = report.groupby(['EECode', 'Firstname', 'Year', 'Week'])['EarnHours'].sum().reset_index()

    # Merge the week_totals dataframe back into the report dataframe
    report = pd.merge(report, week_totals, on=['EECode', 'Firstname', 'Year', 'Week'], suffixes=('', '_week_total'))

    report = report[['EECode', 'Firstname', 'Year', 'Week', 'EarnHours_week_total']]
    report = report.drop_duplicates()

    report['Overtime'] = report['EarnHours_week_total'] - 40

    report = report[report["Overtime"]>0]
    report = report.reset_index()
    report = report[['EECode', 'Firstname', 'Year', 'Week', 'EarnHours_week_total', 'Overtime']]

    print("OT REPORT")
    print(report)

    return(report)

def site_utz_report(timecards):
    report = timecards[['Department', 'InPunchDay', 'EarnHours']]
    report['Year'] = pd.to_datetime(report['InPunchDay']).dt.year
    report['Month'] = pd.to_datetime(report['InPunchDay']).dt.month
    report['Week'] = pd.to_datetime(report['InPunchDay']).dt.week
    report = report[['Department', 'EarnHours', 'Year', 'Month', 'Week']]

    # Group by employee, year, and week and calculate the sum of earned hours for each group
    week_totals = report.groupby(['Department', 'Year', 'Month',
                                  'Week'])['EarnHours'].sum().reset_index()

    # Merge the week_totals dataframe back into the report dataframe
    report = pd.merge(report, week_totals, on=['Department', 'Year', 'Month',
                                               'Week'], suffixes=('', '_total'))

    report = report[['Department', 'Year', 'Month', 'Week', 'EarnHours_total']]
    report = report.drop_duplicates()
    report = report[report['Department'] != 'Administration']

    sites = report.Department.unique()
    site_hours = {
        "site": ["SA8", "W104","SA3","W103","J101","13B Castlebrook","K110"],
        "Approved Hours": [238, 409.01, 208.53, 329, 98, 168, 168]
    }
    sh = pd.DataFrame.from_dict(site_hours)
    report = pd.merge(report, sh, left_on='Department', right_on='site')

    report = report.reset_index()
    report = report[['Department', 'Year', 'Month', 'Week', 'EarnHours_total', 'Approved Hours']]

    report["U.Rate"] = report['EarnHours_total']/report['Approved Hours']

    print("UTZ REPORT")
    print(report.sort_values(["Year","Week"]))

    # Collect Monthly Totals
    monthly_totals = report.groupby(['Department', 'Year', 'Month',
                                     'Approved Hours'])['EarnHours_total'].sum().reset_index()
    monthly_totals = monthly_totals[['Department', 'Year', 'Month', 'Approved Hours', 'EarnHours_total']]
    monthly_totals['Approved Hours'] = (monthly_totals["Approved Hours"]/7)*30
    monthly_totals["U.Rate"] = monthly_totals['EarnHours_total']/monthly_totals['Approved Hours']
    print(monthly_totals)

    return(report, monthly_totals)

def sh_utz(atn, tc, save_path, save_date):

    path = fr"{save_path}\Utilization Report({save_date}).xlsx"

    # using the attendance table, create a dataframe to compare against employee hours
    atn = atn[['program_site',
                'individual',
                'date',
                'attendance']]
    atn.columns = ["Department", "Individual", "Date", "Attendance"]

    # create dataframe columns to categorize date values for further analysis
    atn['week'] = pd.to_datetime(atn['Date']).dt.week
    atn['month'] = pd.to_datetime(atn['Date']).dt.month
    atn['year'] = pd.to_datetime(atn['Date']).dt.year

    # limit the atn dataframe to only the rows that contain "Present" in the attendance column
    atn['Attendance'] = atn['Attendance'].replace(['Present 12 hrs per day'], 'Present')
    atn['Daily Hours'] = 0

    # support hours (sh) dataframe
    sh = pd.DataFrame(columns=[["Individual", "Daily Hours"]])

    # get the individuals names from the attendance dataframe and list them individually
    sh['Individual'] = atn['Individual'].unique()

    # list out a dictionary of the approved daily support hours for each individual served
    ## this may be updated to a separate python file in the future
    support_hours = {
        'Chituck, Christina L': 14,
        'Gallagher, James M': 12,
        'Garrison, Christian': 14,
        'Goldsberry, Nyea Nicole': 13,
        'GREEN, JOSEPH E E': 24,
        'Headen, Deven T': 10,
        'James, Janet M': 33,
        'Jardon-Rosales, Dulce Y': 21,
        'Lanier, Daniel L': 3.79,
        'LeVan, Charles J': 14,
        'Seward, Robert': 46,
        'Wright, Ralph W': 12.43,
        'Faust, Travis A': 14,
        'Wooters, Brianna E': 0
    }

    # assign each individual their respective daily support hours
    atn['Daily Hours'] = atn["Individual"].map(support_hours)

    # Only account for present dates
    report = atn[atn["Attendance"] == "Present"]

    # Weekly Approved Hours
    report = report["Daily Hours"].groupby([atn.Department, atn.year, atn.month, atn.week]).sum().reset_index()

    # Clean up the Department names for each site (this is to make for easier analysis further down)
    report['Department'] = report['Department'].replace(['13B Dartmouth - Castlebrook (13B Dartmouth - Castlebrook)'],
                                                            '13B Castlebrook')
    report['Department'] = report['Department'].replace(['3 Nairn Ln (3 Nairn Ln)'], 'SA3')
    report['Department'] = report['Department'].replace(['324 Broadstairs (Broadstairs E103)'], 'W103')
    report['Department'] = report['Department'].replace(['8 Nairn Ln (8 Nairn Ln)'], 'SA8')
    report['Department'] = report['Department'].replace(['Cannon Mills - 101 (Jeffery Pl CLA)'], 'J101')
    report['Department'] = report['Department'].replace(['Katrina 110 (Katrina 110)'], 'K110')
    report['Department'] = report['Department'].replace(['Westover E104 (Westover-324)'], 'W104')
    report["Approved Hours"] = report["Daily Hours"]

    # create a dataframe from the timecards dataframe
    tc = tc[['Department', 'InPunchDay', 'EarnHours']]
    tc.columns = ["Department", "Date", "EarnHours"]

    print(tc)
    # create dataframe columns to categorize date values for further analysis
    tc['week'] = pd.to_datetime(tc['Date']).dt.week
    tc['month'] = pd.to_datetime(tc['Date']).dt.month
    tc['year'] = pd.to_datetime(tc['Date']).dt.year

    # dataframe that sums the total hours worked by DSP's by Department.
    tc = tc['EarnHours'].groupby([tc.Department, tc.year, tc.month, tc.week]).sum().reset_index()

    # merge the time cards dataframe with the report dataframe to show the total hours worked vs approved per
    # Department
    report = pd.merge(report, tc)

    report = pd.DataFrame(report)

    # The full report displays the sum of all columns throughout the entire report dataframe
    # This shows the utilization rate for the entirety of the data provided
    report = report[['Department','year', 'month', 'week', 'EarnHours', 'Approved Hours']]
    report['Utilization Rate'] = report['EarnHours']/report['Approved Hours']

    return(report)



####### TESTING AREA #######
#path = "C:/Users/olato/OneDrive/Desktop/TOBOLA QA REVIEW/Data_Pulls/2023/4_April/4.10.23/Attendance(4.10.23).csv"
#tc = "C:/Users/olato/OneDrive/Desktop/TOBOLA QA REVIEW/Data_Pulls/2023/4_April/4.10.23/TimeCards(4.10.23).csv"
#save_path = "C:/Users/olato/OneDrive/Desktop/TOBOLA QA REVIEW/Data_Pulls/2023/4_April/4.10.23"
#save_date = '4.10.23'
#atn = pd.read_csv(path)
#tc = pd.read_csv(tc)

#sh_utz(atn, tc, save_path, save_date)