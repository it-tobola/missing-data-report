import appointments
import client_atn
import dsp_reports
import isp_data
import qa_reports
import atn_points
import utz_report
import pandas as pd
import sql_queries as sql
import azure_cnxn as az



date = input("What's Today's Date?:   ")
cut = slice(2)

year = {
    "22": "2022",
    "23": "2023",
    "24": "2024"
}
month = {
    "1.": "1_January",
    "2.": "2_February",
    "3.": "3_March",
    "4.": "4_April",
    "5.": "5_May",
    "6.": "6_June",
    "7.": "7_July",
    "8.": "8_August",
    "9.": "9_September",
    "10": "10_October",
    "11": "11_November",
    "12": "12_December"
}
month_folder = month.get(f'{date[cut]}')
year_folder = year.get(f"{date[-2:]}")

# reference paths for each file needed
save_path = fr"C:\Users\olato\OneDrive\Desktop\TOBOLA QA REVIEW\Data_Pulls\{year_folder}\{month_folder}\{date}"
nc_isp_path = fr"{save_path}\RAW\nc_isp.xlsx"
kc_isp_path = fr"{save_path}\RAW\kc_isp.xlsx"
atn1 = fr"{save_path}\RAW\atn1.xlsx"
atn2 = fr"{save_path}\RAW\atn2.xlsx"
atn3 = fr"{save_path}\RAW\atn3.xlsx"
timecard_path = fr"{save_path}\RAW\timecards.csv"
apt_path = fr"{save_path}\RAW\apts.xlsx"
points_path = fr"{save_path}\RAW\atnpoints.csv"
ee_path = fr"{save_path}\RAW\CurrentEmployees.csv"


# Data Excel File
xlwriter = pd.ExcelWriter(fr"{save_path}\DataReport({date}).xlsx")
wb = xlwriter.book

# create tables from the saved files for analysis and breakdown & load them to a local sql server
isp_table = isp_data.start(kc_isp_path, nc_isp_path, save_path, date)
isp_data.write_to_table(isp_table)

atn_table = client_atn.start(atn1, atn2, atn3, save_path, date)
client_atn.write_to_table(atn_table)

apt_table = appointments.start(apt_path, save_path, date)
appointments.write_to_table(apt_table)

points_table = atn_points.start(points_path, save_path, date)
atn_points.write_to_table(points_table)

ee, notion_ee, timecards = dsp_reports.start(ee_path, timecard_path, save_path, date)
dsp_reports.write_to_table(ee, timecards, save_path)



# run a training compliance report
dsp_reports.training_report(date)

# Missing Data Query
missing_isp_report = sql.mdq(sql.r_filter(atn_table))
counts = missing_isp_report.groupby("Staff Name").size().reset_index(name="Missing ISPs")

aiq = """
SELECT  
    individual,
    date,
    begin_time as 'Time',
    provider,
    specialty,
    apt_status,
    follow_up_date,
    CASE
        WHEN 
            (program='3 Nairn Ln' OR program='8 Nairn Ln')
            THEN 'David'
        WHEN
            (program='324 Broadstairs' OR program='Westover E104' OR program='13B Dartmouth - Castlebrook')
            THEN 'Teena'
        WHEN
            (program='Katrina 110' OR program='Cannon Mills - 101')
            THEN 'Whitney'
        END AS 'Manager',
    CASE 
        WHEN
            (program='3 Nairn Ln' OR program='8 Nairn Ln' OR program='13B Dartmouth - Castlebrook')
            THEN 'New Castle County'
        WHEN
            (program='324 Broadstairs' OR program='Westover E104' OR program='104 Katrina Way' OR program='Cannon Mills - 101' OR program='Katrina 110')
            THEN 'Kent County'
        END AS 'County'

FROM
    Appointments2022

WHERE
    (apt_status='Scheduled')
    OR
    (apt_status='Cancelled' AND (Comment is null))
    OR 
    (apt_status='Rescheduled' AND follow_up_date is null)
    OR
    (apt_status='Declined' AND (follow_up_date is null OR Comment is null))
    OR 
    (apt_status='Not Scheduled' AND (comment is null OR [Description] is null))"""
apt_data = pd.read_sql_query(aiq, con=az.engine)

# Attendance Points Query
apq = """
SELECT
    EE_Code as [Employee ID],
    concat(pt.FirstName, ' ', pt.LastName) as 'Staff',
    Count(pt.Points)  as 'Points',
    AVG(pt.Minutes_Points_Off) as 'Average Time Late'
    
FROM
    atnPoints pt
    Join NEW n 
    ON pt.EE_Code=n.Employee_Code

GROUP BY 
    EE_Code, 
    pt.FirstName,
	pt.LastName

Order by 
	ee_code
    """
ap_data = pd.read_sql_query(apq, con=az.engine)


# Overtime Report
ot = dsp_reports.ot_report(pd.read_csv(timecard_path))

# Utilization report
utz = utz_report.utz(timecards)

# QA Reports
qa_df = qa_reports.qa_df

## ISP DATA
missing_isp_report.to_excel(xlwriter, sheet_name="ISPs", index=False)
counts.to_excel(xlwriter, sheet_name="ISP Counts", index=False)
apt_data.to_excel(xlwriter, sheet_name="Apts", index=False)
ot.to_excel(xlwriter, sheet_name="Staff_OT", index=False)
utz.to_excel(xlwriter, sheet_name="UTZ", index=False)
ap_data.to_excel(xlwriter, sheet_name="Attendance_Points", index=False)
notion_ee.to_excel(xlwriter, sheet_name="Current_EE", index=False)
qa_df.to_excel(xlwriter, sheet_name="QA Reports", index=False)
xlwriter.close()

# Display the staff who are currently missing documentation, and how many documentations are they missing
print(counts)
print(missing_isp_report)

