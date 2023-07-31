import pandas as pd
import azure_cnxn as az
from sqlalchemy import create_engine


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