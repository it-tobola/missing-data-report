import dsp_reports
import qa_reports
import pandas as pd


notion_token = "secret_omL8nzIdOZySUeAtSOCHm0bUNh2ydXdohuePKPBXkxm"
ee_db_id = "03764697bdf74f2b938313815cf62069"
ee_url = "https://www.notion.so/03764697bdf74f2b938313815cf62069?v=e856d446c1a44cfcb8857b014f591284"
qa_db_id = "b408c4f33b91448595c640f8d0b3dd21"
qa_url = "https://www.notion.so/b408c4f33b91448595c640f8d0b3dd21?v=ca756db095b941b4981dbf0839b0de73&pvs=4"

def start(nc_isp, kc_isp, atn1, atn2, atn3, apts, timecards, ee):

    # ISP TABLE
    nc_isp = pd.read_excel(nc_isp)
    kc_isp = pd.read_excel(kc_isp)

    isp = nc_isp.append(kc_isp, ignore_index=True)

    ###DATA CLEANING

    # un-needed columns
    isp = isp.drop(['Site Name', 'Status', 'EVV Supporting Document', 'Duration (hh:mm)'], axis=1)

    # rename columns & change data types for SQL analysis
    isp.columns = ['form_id',
                       'site',
                       'individual',
                       "isp_program",
                       'entered_by',
                       'date',
                       'billable',
                       'begin_time',
                       'end_time',
                       'duration',
                       'location',
                       'comments',
                       'group_count',
                       'time_zone']

    isp.date = pd.to_datetime(isp['date'])
    isp.begin_time = pd.to_datetime(isp.begin_time).dt.time
    isp.end_time = pd.to_datetime(isp.end_time).dt.time

    #### Clean Individual Names ####

    ## E103
    isp['individual'] = isp['individual'].replace(["James, Janet"], 'James, Janet M')
    isp['individual'] = isp['individual'].replace(["Chituck, Christina"], 'Chituck, Christina L')
    isp['individual'] = isp['individual'].replace(["Wooters, Brianna"], 'Wooters, Brianna E')
    ## E104
    isp['individual'] = isp['individual'].replace(["Wright, Ralph"], 'Wright, Ralph W')
    isp['individual'] = isp['individual'].replace(["Seward, Robert"], 'Seward, Robert')
    ## J101
    isp['individual'] = isp['individual'].replace(["LeVan, Charles"], 'LeVan, Charles J')
    ## K110
    isp['individual'] = isp['individual'].replace(["GREEN, JOSEPH E"], 'GREEN, JOSEPH E E')
    ## 3NL
    isp['individual'] = isp['individual'].replace(["Gallagher, James"], 'Gallagher, James M')
    isp['individual'] = isp['individual'].replace(["Garrison, Christian"], 'Garrison, Christian')
    isp['individual'] = isp['individual'].replace(["Lanier, Daniel"], 'Lanier, Daniel L')
    ## 8NL
    isp['individual'] = isp['individual'].replace(["Jardon-Rosales, Dulce"], 'Jardon-Rosales, Dulce Y')
    isp['individual'] = isp['individual'].replace(["Goldsberry, Nyea"], 'Goldsberry, Nyea Nicole')
    ## Castlebrook
    isp['individual'] = isp['individual'].replace(["Faust, Travis"], 'Faust, Travis A')
    isp['individual'] = isp['individual'].replace(["Headen, Deven"], 'Headen, Deven T')

    ############################################################## End

    # ATTENDANCE
    q1 = pd.read_excel(atn1)
    q2 = pd.read_excel(atn2)
    q3 = pd.read_excel(atn3)

    # Create dataframes for the past 6 months
    atn = pd.concat([q1, q2, q3], ignore_index=True)

    # Update Column Names
    atn.columns = ['program_site',
                   'individual',
                   'date',
                   'attendance',
                   'status',
                   'entered_date',
                   'entered_by',
                   'time_zone']

    # Update necessary column datatypes/values
    atn.date = pd.to_datetime(atn.date)
    atn['entered_date'] = pd.to_datetime(atn['entered_date'])

    # Data Cleaning (Individual Names)

    ## E103
    atn['individual'] = atn['individual'].replace(["James, Janet"], 'James, Janet M')
    atn['individual'] = atn['individual'].replace(["Chituck, Christina"], 'Chituck, Christina L')
    atn['individual'] = atn['individual'].replace(["Wooters, Brianna"], 'Wooters, Brianna E')

    ## E104
    atn['individual'] = atn['individual'].replace(["Wright, Ralph"], 'Wright, Ralph W')
    atn['individual'] = atn['individual'].replace(["Seward, Robert"], 'Seward, Robert')

    ## J101
    atn['individual'] = atn['individual'].replace(["LeVan, Charles"], 'LeVan, Charles J')

    ## K110
    atn['individual'] = atn['individual'].replace(["GREEN, JOSEPH E"], 'GREEN, JOSEPH E E')

    ## 3NL
    atn['individual'] = atn['individual'].replace(["Gallagher, James"], 'Gallagher, James M')
    atn['individual'] = atn['individual'].replace(["Garrison, Christian"], 'Garrison, Christian')
    atn['individual'] = atn['individual'].replace(["Lanier, Daniel"], 'Lanier, Daniel L')

    ## 8NL
    atn['individual'] = atn['individual'].replace(["Jardon-Rosales, Dulce"], 'Jardon-Rosales, Dulce Y')
    atn['individual'] = atn['individual'].replace(["Goldsberry, Nyea"], 'Goldsberry, Nyea Nicole')

    ## Castlebrook
    atn['individual'] = atn['individual'].replace(["Faust, Travis"], 'Faust, Travis A')
    atn['individual'] = atn['individual'].replace(["Headen, Deven"], 'Headen, Deven T')


    atn.drop_duplicates

    ########################################################################################## END


    # APPOINTMENTS

    apts = pd.read_excel(apts)

    # Update Column names
    apts.columns = ['Form_ID',
                   'program',
                   'Site',
                   'individual',
                   'Entered_By',
                   'Reported_By',
                   'date',
                   'begin_time',
                   'End_Time',
                   'provider',
                   'specialty',
                   'reason',
                   'description',
                   'Location_Type',
                   'follow_up_date',
                   'Address',
                   'Location',
                   'Phone',
                   'Driver',
                   'Pick Up At',
                   'Depart Time',
                   'Status',
                   'Notification Level',
                   'apt_status',
                   'comment',
                   'time_zone']

    # Drop unnecessary columns
    apts.drop(columns=['Site',
                      'Entered_By',
                      'Reported_By',
                      'End_Time',
                      'Location_Type',
                      'Address',
                      'Location',
                      'Phone',
                      'Driver',
                      'Pick Up At',
                      'Depart Time',
                      'Status',
                      'Notification Level'], axis=1)

    apts["date"] = pd.to_datetime(apts.date)
    apts["begin_time"] = pd.to_datetime(apts['begin_time']).dt.time

    ########################################################################################## END

    # TIMECARDS

    # Create a dataframe from the excel file
    tc = pd.read_csv(timecards)

    # Separate the time punch columns into 2 separate entities (date, time)

    ## InPunch
    in_dates = pd.DataFrame(tc.InPunchTime.str.split(" ", expand=True, n=1)[0])
    in_time = pd.DataFrame(tc.InPunchTime.str.split(" ", expand=True, n=1)[1])
    tc['InPunchDay'] = in_dates
    tc['InPunchTime'] = in_time

    ## OutPunch
    out_dates = pd.DataFrame(tc.OutPunchTime.str.split(" ", expand=True, n=1)[0])
    out_time = pd.DataFrame(tc.OutPunchTime.str.split(" ", expand=True, n=1)[1])
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

    timecards = tc

    ########################################################################################## END

    # CURRENT EMPLOYEES

   # Create a dataframe from the excel file
    ee = pd.read_csv(ee)

    ee.to_csv(ee)

    ee['EE Code'] = ee['Employee_Code']
    ee['First Name'] = ee['Legal_Firstname']
    ee['Last Name'] = ee['Legal_Lastname']
    ee['Hire Date'] = ee['Hire_Date']
    ee['Position Seat'] = ee['Position_Seat_Number']
    ee['Termination Date'] = ee['Termination_Date']

    notion_ee = create_ee_table.ee_df_trimmed

    ########################################################################################## END


    return(isp, atn, apts, timecards, ee, notion_ee)

def write_to_table(isp, atn, apts, timecards, ee, notion_ee):

    ##  Create Table
    import azure_cnxn as az
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL
    cnxn_url = URL.create("mssql+pyodbc", query={"odbc_connect": az.cnxn_string})
    engine = create_engine(cnxn_url)

    isp.to_sql("ISP", engine, index=False, if_exists='replace')
    atn.to_sql("Attendance2022", engine, index=False, if_exists='replace')
    apts.to_sql("Appointments2022", engine, index=False, if_exists='replace')
    notion_ee.to_sql("OLD", engine, index=False, if_exists='replace')
    ee.to_sql("NEW", engine, index=False, if_exists='replace')
    timecards.to_sql("TimeCards2022", engine, index=False, if_exists='replace')

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
    updates.to_notion

    # Add new employees to notion ee database
    updates.to_notion(ee_url, title="Tests", api_key=notion_token)

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

    discrepancies = catch_discrepancies(old=notion_ee, new=ee)

    mdq = """

    --DEVEN HEADEN --
    (Select 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        tc.OutPunchTime as 'Shift End',
        'Castlebrook' as 'Home',
        'Teena' as 'Manager', 
        'New Castle County' as 'County'

    From 
        Attendance2022 atn

        LEFT JOIN isp
            ON atn.date=isp.date 
            AND isp.Individual=atn.individual
            AND ((cast(isp.begin_time as time)>='14:00' AND isp.[duration]>120)
                AND (cast(isp.begin_time as time)<='17:00' AND isp.[duration]>120))

        Left Join TimeCards2022 tc 
            ON atn.date=tc.InPunchDay
            AND (tc.EarnCode = 'R' OR tc.EarnCode is null)
            AND tc.Department='13B Castlebrook'

        WHERE 
            atn.individual like 'HEAD%'
            AND isp.isp_program is NULL


    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        tc.InPunchTime>='14:00'
        AND tc.InPunchTime<='17:00'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3


    )

    UNION
    (Select 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        '11:59 PM' as 'Shift End',
        'Castlebrook' as 'Home',
        'Teena' as 'Manager', 
        'New Castle County' as 'County'

    FROM 
        [Attendance2022] atn

            Left Join isp
                ON (atn.date=isp.date)
                AND (isp.Individual=atn.individual)
                AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>=30)
                    AND (cast(isp.begin_time as time)<='11:59pm' AND isp.[duration]>=30))

            Left Join   TimeCards2022 tc
                ON (atn.date=tc.InPunchDay)
                AND tc.Department='13B Castlebrook'

    WHERE 
        atn.individual like 'HEAD%'
        AND isp.isp_program is NULL

    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING cast(tc.InPunchTime as time)>='6pm'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3


        )

        UNION
        (Select 
        atn.individual as 'Name',
        tc.OutPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        '12:00 AM' as 'Shift Start', 
        tc.OutPunchTime as 'Shift End',
        'Castlebrook' as 'Home',
        'Teena' as 'Manager', 
        'New Castle County' as 'County'

    FROM 
        [Attendance2022] atn

            Left Join isp
                ON (atn.date=isp.date)
                AND (isp.Individual=atn.individual)
                AND ((cast(isp.begin_time as time)='12:00 AM' AND isp.[duration]>=30))

            Left Join TimeCards2022 tc
                ON (atn.date=tc.OutPunchDay)
                AND tc.Department='13B Castlebrook'

    WHERE 
        atn.individual like 'HEAD%'
        AND isp.isp_program is NULL

    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING cast(tc.InPunchTime as time)>='6pm'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3      

    )

    -- TRAVIS FAUST --
    UNION
    (Select 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        tc.OutPunchTime as 'Shift End',
        'Castlebrook' as 'Home',
        'Teena' as 'Manager', 
        'New Castle County' as 'County'

    From 
        Attendance2022 atn

        LEFT JOIN isp
            ON atn.date=isp.date 
            AND isp.Individual=atn.individual
            AND ((cast(isp.begin_time as time)>='14:00' AND isp.[duration]>120)
                    AND (cast(isp.begin_time as time)<='17:00' AND isp.[duration]>120))

        Left Join TimeCards2022 tc 
            ON atn.date=tc.InPunchDay
            AND (tc.EarnCode = 'R' OR tc.EarnCode is null)
            AND tc.Department='13B Castlebrook'

        WHERE 
            atn.individual like 'FAUST%'
            AND isp.isp_program is NULL


    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        tc.InPunchTime>='14:00'
        AND tc.InPunchTime<='17:00'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3


    )

    UNION
    (Select 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        '11:59 PM' as 'Shift End',
        'Castlebrook' as 'Home',
        'Teena' as 'Manager', 
        'New Castle County' as 'County'

    FROM 
        [Attendance2022] atn

            Left Join isp
                ON (atn.date=isp.date)
                AND (isp.Individual=atn.individual)
                AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>=30)
                    AND (cast(isp.begin_time as time)<='11:59pm' AND isp.[duration]>=30))

            Left Join   TimeCards2022 tc
                ON (atn.date=tc.InPunchDay)
                AND tc.Department='13B Castlebrook'

    WHERE 
        atn.individual like 'FAUST%'
        AND isp.isp_program is NULL

    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING cast(tc.InPunchTime as time)>='6pm'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3


        )

        UNION
        (Select 
        atn.individual as 'Name',
        tc.OutPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        '12:00 AM' as 'Shift Start', 
        tc.OutPunchTime as 'Shift End',
        'Castlebrook' as 'Home',
        'Teena' as 'Manager', 
        'New Castle County' as 'County'

    FROM 
        [Attendance2022] atn

            Left Join isp
                ON (atn.date=isp.date)
                AND (isp.Individual=atn.individual)
                AND ((cast(isp.begin_time as time)='12:00 AM' AND isp.[duration]>=30))

            Left Join TimeCards2022 tc
                ON (atn.date=tc.OutPunchDay)
                AND tc.Department='13B Castlebrook'

    WHERE 
        atn.individual like 'FAUST%'
        AND isp.isp_program is NULL

    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING cast(tc.InPunchTime as time)>='6pm'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3



    )

    -- CHRISTIAN GARRISON --
    UNION
    (Select 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        tc.OutPunchTime as 'Shift End',
        '3 Nairn' as 'Home', 
        'David' as 'Manager',
        'New Castle County' as 'County'

    FROM 
        [attendance2022] atn

        left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)>='07:00' AND isp.[duration]>120)
            AND (cast(isp.begin_time as time)<='10:00' AND isp.[duration]>120))

        Left Join TimeCards2022 tc 
            ON atn.date=tc.InPunchDay
            AND (tc.EarnCode = 'R' OR tc.EarnCode is null)
            AND tc.Department='SA3'

    WHERE 
        atn.individual like 'GARR%'
        AND isp.isp_program is NULL
        -- IF THE INDIVIDUAL GOES TO DAY PROGRAM --
        AND (datepart(weekday,atn.date)<2 OR datepart(weekday,atn.date)>6) 

    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        cast(tc.InPunchTime as time)>='07:00'
        AND cast(tc.InPunchTime as time)<='10:00'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3







    )
    UNION
    (Select 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        tc.OutPunchTime as 'Shift End',
        '3 Nairn' as 'Home', 
        'David' as 'Manager',
        'New Castle County' as 'County'


    FROM [attendance2022] atn

        left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)>='14:00' AND isp.[duration]>120)
            AND (cast(isp.begin_time as time)<='17:00' AND isp.[duration]>120))

        Left Join TimeCards2022 tc 
            ON atn.date=tc.InPunchDay
            AND (tc.EarnCode = 'R' OR tc.EarnCode is null)
            AND tc.Department='SA3'

    WHERE 
        atn.individual like 'GARR%'
        AND isp.isp_program is NULL


    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        cast(tc.InPunchTime as time)>='14:00'
        AND cast(tc.InPunchTime as time)<='17:00'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3







    )
    UNION
    (Select 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        '11:59 PM' as 'Shift End',
        '3 Nairn' as 'Home',
        'David' as 'Manager',
        'New Castle County' as 'County'

    FROM [attendance2022] atn

        left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>=30)
            AND (cast(isp.begin_time as time)<='11:59pm' AND isp.[duration]>=30))

        Left Join TimeCards2022 tc
            ON (atn.date=tc.InPunchDay)
            AND tc.Department='SA3'

    WHERE 
        atn.individual like 'GARR%'
        AND isp.isp_program is NULL

    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        cast(tc.InPunchTime as time)>='6pm'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3






    )
    UNION
    (Select 
        atn.individual as 'Name',
        tc.OutPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        '12:00 AM' as 'Shift Start', 
        tc.OutPunchTime as 'Shift End',
        '3 Nairn' as 'Home', 
        'David' as 'Manager',
        'New Castle County' as 'County'

    FROM [attendance2022] atn

        left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)='12am' AND isp.[duration]>=30))

        Left Join TimeCards2022 tc
            ON (atn.date=tc.OutPunchDay)
            AND tc.Department='SA3'

    WHERE atn.individual like 'GARR%'
        AND isp.isp_program is NULL

    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        cast(tc.InPunchTime as time)>='6pm'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3







    )

    -- DANIEL LANIER -- 

    UNION
    (Select 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        tc.OutPunchTime as 'Shift End',
        '3 Nairn' as 'Home', 
        'David' as 'Manager',
        'New Castle County' as 'County'

    FROM 
        [attendance2022] atn

        left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)>='07:00' AND isp.[duration]>120)
            AND (cast(isp.begin_time as time)<='10:00' AND isp.[duration]>120))

        Left Join TimeCards2022 tc 
            ON atn.date=tc.InPunchDay
            AND (tc.EarnCode = 'R' OR tc.EarnCode is null)
            AND tc.Department='SA3'

    WHERE 
        atn.individual like 'LANI%'
        AND isp.isp_program is NULL
        -- IF THE INDIVIDUAL GOES TO DAY PROGRAM --
        AND (datepart(weekday,atn.date)<2 OR datepart(weekday,atn.date)>6) 

    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        cast(tc.InPunchTime as time)>='07:00'
        AND cast(tc.InPunchTime as time)<='10:00'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3






    )
    UNION
    (Select 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        tc.OutPunchTime as 'Shift End',
        '3 Nairn' as 'Home', 
        'David' as 'Manager',
        'New Castle County' as 'County'


    FROM [attendance2022] atn

        left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)>='14:00' AND isp.[duration]>120)
            AND (cast(isp.begin_time as time)<='17:00' AND isp.[duration]>120))

        Left Join TimeCards2022 tc 
            ON atn.date=tc.InPunchDay
            AND (tc.EarnCode = 'R' OR tc.EarnCode is null)
            AND tc.Department='SA3'

    WHERE 
        atn.individual like 'LANI%'
        AND isp.isp_program is NULL


    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        cast(tc.InPunchTime as time)>='14:00'
        AND cast(tc.InPunchTime as time)<='17:00'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3






    )
    UNION
    (Select 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        '11:59 PM' as 'Shift End',
        '3 Nairn' as 'Home',
        'David' as 'Manager',
        'New Castle County' as 'County'

    FROM [attendance2022] atn

        left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>=30)
            AND (cast(isp.begin_time as time)<='11:59pm' AND isp.[duration]>=30))

        Left Join TimeCards2022 tc
            ON (atn.date=tc.InPunchDay)
            AND tc.Department='SA3'

    WHERE 
        atn.individual like 'LANI%'
        AND isp.isp_program is NULL

    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        cast(tc.InPunchTime as time)>='6pm'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3






    )

    UNION
    (Select 
        atn.individual as 'Name',
        tc.OutPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        '12:00 AM' as 'Shift Start', 
        tc.OutPunchTime as 'Shift End',
        '3 Nairn' as 'Home', 
        'David' as 'Manager',
        'New Castle County' as 'County'

    FROM [attendance2022] atn

        left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)='12am' AND isp.[duration]>=30))

        Left Join TimeCards2022 tc
            ON (atn.date=tc.OutPunchDay)
            AND tc.Department='SA3'

    WHERE atn.individual like 'LANI%'
    AND isp.isp_program is NULL

    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        cast(tc.InPunchTime as time)>='6pm'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3







    )

    -- JAMES GALLAGHER --
    UNION
    (Select 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        tc.OutPunchTime as 'Shift End',
        '3 Nairn' as 'Home', 
        'David' as 'Manager',
        'New Castle County' as 'County'

    FROM 
        [attendance2022] atn

        left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)>='07:00' AND isp.[duration]>120)
            AND (cast(isp.begin_time as time)<='10:00' AND isp.[duration]>120))

        Left Join TimeCards2022 tc 
            ON atn.date=tc.InPunchDay
            AND (tc.EarnCode = 'R' OR tc.EarnCode is null)
            AND tc.Department='SA3'

    WHERE 
        atn.individual like 'GALL%'
        AND isp.isp_program is NULL
        -- IF THE INDIVIDUAL GOES TO DAY PROGRAM --
        AND (datepart(weekday,atn.date)<2 OR datepart(weekday,atn.date)>6) 

    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        cast(tc.InPunchTime as time)>='07:00'
        AND cast(tc.InPunchTime as time)<='10:00'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3






    )
    UNION
    (Select 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        tc.OutPunchTime as 'Shift End',
        '3 Nairn' as 'Home', 
        'David' as 'Manager',
        'New Castle County' as 'County'


    FROM [attendance2022] atn

        left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)>='14:00' AND isp.[duration]>120)
            AND (cast(isp.begin_time as time)<='17:00' AND isp.[duration]>120))

        Left Join TimeCards2022 tc 
            ON atn.date=tc.InPunchDay
            AND (tc.EarnCode = 'R' OR tc.EarnCode is null)
            AND tc.Department='SA3'

    WHERE 
        atn.individual like 'GALL%'
        AND isp.isp_program is NULL


    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        cast(tc.InPunchTime as time)>='14:00'
        AND cast(tc.InPunchTime as time)<='17:00'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3






    )
    UNION
    (Select 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        '11:59 PM' as 'Shift End',
        '3 Nairn' as 'Home',
        'David' as 'Manager',
        'New Castle County' as 'County'

    FROM [attendance2022] atn

        left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>=30)
            AND (cast(isp.begin_time as time)<='11:59pm' AND isp.[duration]>=30))

        Left Join TimeCards2022 tc
            ON (atn.date=tc.InPunchDay)
            AND tc.Department='SA3'

    WHERE 
        atn.individual like 'GALL%'
        AND isp.isp_program is NULL

    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        cast(tc.InPunchTime as time)>='6pm'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3






    )
    UNION
    (Select 
        atn.individual as 'Name',
        tc.OutPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        '12:00 AM' as 'Shift Start', 
        tc.OutPunchTime as 'Shift End',
        '3 Nairn' as 'Home', 
        'David' as 'Manager',
        'New Castle County' as 'County'

    FROM [attendance2022] atn

        left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)='12am' AND isp.[duration]>=30))

        Left Join TimeCards2022 tc
            ON (atn.date=tc.OutPunchDay)
            AND tc.Department='SA3'

    WHERE atn.individual like 'GALL%'
    AND isp.isp_program is NULL

    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        cast(tc.InPunchTime as time)>='6pm'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3






    )

    -- DULCE JARDON-ROSALES -- 
    UNION
    (Select 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        tc.OutPunchTime as 'Shift End',
        '8 Nairn' as 'Home', 
        'David' as 'Manager',
        'New Castle County' as 'County'

    FROM 
        [attendance2022] atn

        left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)>='07:00' AND isp.[duration]>120)
            AND (cast(isp.begin_time as time)<='10:00' AND isp.[duration]>120))

        Left Join TimeCards2022 tc 
            ON atn.date=tc.InPunchDay
            AND (tc.EarnCode = 'R' OR tc.EarnCode is null)
            AND tc.Department='SA8'

    WHERE 
        atn.individual like 'JARD%'
        AND isp.isp_program is NULL
        -- IF THE INDIVIDUAL GOES TO DAY PROGRAM --
        AND (datepart(weekday,atn.date)<2 OR datepart(weekday,atn.date)>6) 

    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        cast(tc.InPunchTime as time)>='07:00'
        AND cast(tc.InPunchTime as time)<='10:00'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3






    )
    UNION
    (Select 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        tc.OutPunchTime as 'Shift End',
        '8 Nairn' as 'Home', 
        'David' as 'Manager',
        'New Castle County' as 'County'


    FROM [attendance2022] atn

        left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)>='14:00' AND isp.[duration]>120)
            AND (cast(isp.begin_time as time)<='17:00' AND isp.[duration]>120))

        Left Join TimeCards2022 tc 
            ON atn.date=tc.InPunchDay
            AND (tc.EarnCode = 'R' OR tc.EarnCode is null)
            AND tc.Department='SA8'

    WHERE 
        atn.individual like 'JARD%'
        AND isp.isp_program is NULL


    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        cast(tc.InPunchTime as time)>='14:00'
        AND cast(tc.InPunchTime as time)<='17:00'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3






    )
    UNION
    (Select 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        '11:59 PM' as 'Shift End',
        '8 Nairn' as 'Home',
        'David' as 'Manager',
        'New Castle County' as 'County'

    FROM [attendance2022] atn

        left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>=30)
            AND (cast(isp.begin_time as time)<='11:59pm' AND isp.[duration]>=30))

        Left Join TimeCards2022 tc
            ON (atn.date=tc.InPunchDay)
            AND tc.Department='SA8'

    WHERE 
        atn.individual like 'JARD%'
        AND isp.isp_program is NULL

    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        cast(tc.InPunchTime as time)>='6pm'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3






    )
    UNION
    (Select 
        atn.individual as 'Name',
        tc.OutPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        '12:00 AM' as 'Shift Start', 
        tc.OutPunchTime as 'Shift End',
        '8 Nairn' as 'Home', 
        'David' as 'Manager',
        'New Castle County' as 'County'

    FROM [attendance2022] atn

        left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)='12am' AND isp.[duration]>=30))

        Left Join TimeCards2022 tc
            ON (atn.date=tc.OutPunchDay)
            AND tc.Department='SA8'

    WHERE atn.individual like 'JARD%'
    AND isp.isp_program is NULL

    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        cast(tc.InPunchTime as time)>='6pm'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3







    )

    -- ROBERT SEWARD --
    UNION
    (Select 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        tc.OutPunchTime as 'Shift End',
                    'E104' AS 'Home',
                    'Teena' as 'Manager',
                    'Kent County' as 'County'

        FROM [attendance2022] atn


                left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)>='07:00' AND isp.[duration]>120)
            AND (cast(isp.begin_time as time)<='09:00' AND isp.[duration]>120))


        Left Join TimeCards2022 tc 
            ON atn.date=tc.InPunchDay
            AND (tc.EarnCode = 'R' OR tc.EarnCode is null)
                AND tc.Department='W104'

        WHERE atn.individual like 'SEWARD%'
                AND isp.isp_program is NULL
                 -- IF THE INDIVIDUAL GOES TO DAY PROGRAM --
               -- AND (datepart(weekday,atn.date)<2 OR datepart(weekday,atn.date)>6) 

    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        datepart(hour, tc.InPunchTime)>=7
        AND datepart(hour, tc.InPunchTime)<=10
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3






    )
    UNION
    (SELECT 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        tc.OutPunchTime as 'Shift End',
                    'E104' AS 'Home',
                    'Teena' as 'Manager',
                    'Kent County' as 'County'

        FROM [attendance2022] atn

                 left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)>='14:00' AND isp.[duration]>120)
        AND (cast(isp.begin_time as time)<='17:00' AND isp.[duration]>120))

        Left Join TimeCards2022 tc 
        ON atn.date=tc.InPunchDay
            AND (tc.EarnCode = 'R' OR tc.EarnCode is null)
                AND tc.Department='W104'

        WHERE atn.individual like 'SEWARD%'
                AND isp.isp_program is NULL

    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        cast(tc.InPunchTime as time)>='14:00'
        AND cast(tc.InPunchTime as time)<='17:00'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3






    )
    UNION
    (Select 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        '11:59 PM' as 'Shift End',
                    'E104' AS 'Home',
                    'Teena' as 'Manager',
                    'Kent County' as 'County'

        FROM [attendance2022] atn

        left Join isp
                ON (atn.date=isp.date)
                AND (isp.Individual=atn.individual)
                AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>=30)
                    AND (cast(isp.begin_time as time)<='11:59pm' AND isp.[duration]>=30))

        Left Join TimeCards2022 tc
            ON (atn.date=tc.InPunchDay)
                AND tc.Department='W104'

        WHERE atn.individual like 'SEWARD%'
               AND isp.isp_program is NULL

    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        cast(tc.InPunchTime as time)>='6pm'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3






    )
    UNION
    (Select 
        atn.individual as 'Name',
        tc.OutPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        '12:00 AM' as 'Shift Start', 
        tc.OutPunchTime as 'Shift End', 
                    'E104' AS 'Home',
                    'Teena' as 'Manager',
                    'Kent County' as 'County'
        FROM [attendance2022] atn

              left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)='12am' AND isp.[duration]>=30))

        Left Join TimeCards2022 tc
        ON (atn.date=tc.OutPunchDay)
                AND tc.Department='W104'

        WHERE atn.individual like 'SEWARD%'
               AND isp.isp_program is NULL

    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay


    HAVING 
        cast(tc.InPunchTime as time)>='6pm'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3






    )

    -- CHARLES LEVAN -- 
    UNION
    (SELECT
    	atn.individual as 'Name',
    	atn.Date as 'Date',
    	concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
    	datename(weekday, atn.date) as 'Weekday', 
    	tc.InPunchTime as 'Shift Start', 
    	tc.OutPunchTime as 'Shift End',
                    'J101' as 'Home',
                    'Whitney' as 'Manager',
                    'Kent County' as 'County'


    	From TOBOLA..[isp] isp
                    Right Join TOBOLA..[attendance2022] atn
                        On atn.date=isp.[date] AND atn.individual=isp.individual
                    Right Join TimeCards2022 tc
                        On (concat(datename(weekday, tc.InPunchDay), ', ',datename(MONTH, tc.InPunchDay),' ', datename(day, tc.InPunchDay),', ', datename(year, tc.InPunchDay)))
    	=
    	concat(datename(weekday, atn.date), ', ',datename(MONTH, atn.date),' ', datename(day, atn.date),', ', datename(year, atn.date))

        Where (atn.Program_Site like '324%' or atn.Program_Site like '104%' or atn.Program_Site like '%101%' or atn.Program_Site like '%110%' or atn.Program_Site like 'west%')
                      AND atn.attendance like '%12%'
        AND tc.EarnHours > 3 AND isp.date is null
                    AND atn.individual like 'levan%'
                    AND tc.Department='J101'AND (tc.EarnCode='R' OR tc.EarnCode is null)


    Group By atn.date, tc.EarnHours,

    	atn.individual, 
    	isp.[date], 
    	atn.date, 
    	atn.attendance, 
    	tc.Firstname, 
    	tc.Lastname, 
    	tc.InPunchTime, 
    	tc.OutPunchTime, 
    	TC.InPunchDay


    )

    -- JOSEPH GREEN -- 
    UNION
    (Select 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        tc.OutPunchTime as 'Shift End',
        'K110' as 'Home',
        'Whitney' as 'Manager',
        'Kent County' as 'County'

    FROM [Attendance2022] atn

        left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)>='07:00' AND isp.[duration]>120)
            AND (cast(isp.begin_time as time)<='09:00' AND isp.[duration]>120))


        Left Join TimeCards2022 tc 
            ON atn.date=tc.InPunchDay
            AND (tc.EarnCode = 'R' OR tc.EarnCode is null)
            AND tc.Department='k110'

    WHERE 
        atn.individual like 'gree%'
        AND isp.isp_program is NULL
        -- IF THE INDIVIDUAL GOES TO DAY PROGRAM --
        -- AND (datepart(weekday,atn.date)<2 OR datepart(weekday,atn.date)>6) 

    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        datepart(hour, tc.InPunchTime)>=7
        AND datepart(hour, tc.InPunchTime)<=10
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3





    )
    UNION
    (Select 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        tc.OutPunchTime as 'Shift End',
        'K110' as 'Home',
        'Whitney' as 'Manager',
        'Kent County' as 'County'


    FROM [Attendance2022] atn

        left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)>='14:00' AND isp.[duration]>120)
        AND (cast(isp.begin_time as time)<='17:00' AND isp.[duration]>120))

        Left Join TimeCards2022 tc 
        ON atn.date=tc.InPunchDay
            AND (tc.EarnCode = 'R' OR tc.EarnCode is null)
        AND tc.Department='k110'

    WHERE 
        atn.individual like 'gree%'
        AND isp.isp_program is NULL


    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        cast(tc.InPunchTime as time)>='14:00'
        AND cast(tc.InPunchTime as time)<='17:00'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3



    )
    UNION
    (Select 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        '11:59 PM' as 'Shift End',
        'K110' as 'Home',
        'Whitney' as 'Manager',
        'Kent County' as 'County'

    FROM [Attendance2022] atn

        left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>=30)
        AND (cast(isp.begin_time as time)<='11:59pm' AND isp.[duration]>=30))

        Left Join TimeCards2022 tc
        ON (atn.date=tc.InPunchDay)
        AND tc.Department='k110'

    WHERE 
        atn.individual like 'gree%'
        AND isp.isp_program is NULL

    Group By atn.date, tc.EarnHours,

    atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance

    HAVING 
        cast(tc.InPunchTime as time)>='6pm'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3



    )
    UNION
    (Select 
        atn.individual as 'Name',
        tc.OutPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        '12:00 AM' as 'Shift Start', 
        tc.OutPunchTime as 'Shift End',                                                                                                                                                                                                                                                           
        'K110' as 'Home',
        'Whitney' as 'Manager',
        'Kent County' as 'County'

    FROM [Attendance2022] atn

        left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)='12am' AND isp.[duration]>=30))

        Left Join TimeCards2022 tc
        ON (atn.date=tc.OutPunchDay)
        AND tc.Department='k110'

    WHERE 
        atn.individual like 'gree%'
        AND isp.isp_program is NULL


    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay


    HAVING 
        cast(tc.InPunchTime as time)>='6pm'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3





    )

    -- JANET JAMES -- 
    UNION
    (Select 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        tc.OutPunchTime as 'Shift End',
                    'E103' as 'Home',
                    'Teena' as 'Manager',
                    'Kent County' as 'County'
        FROM [Attendance2022] atn

                left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)>='07:00' AND isp.[duration]>120)
            AND (cast(isp.begin_time as time)<='09:00' AND isp.[duration]>120))


        Left Join TimeCards2022 tc 
            ON atn.date=tc.InPunchDay
            AND (tc.EarnCode = 'R' OR tc.EarnCode is null)
                AND tc.Department='W103'

        WHERE atn.individual like 'JAMES%'
                AND isp.isp_program is NULL
                 -- IF THE INDIVIDUAL GOES TO DAY PROGRAM --
                AND (datepart(weekday,atn.date)<2 OR datepart(weekday,atn.date)>6) 
    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        datepart(hour, tc.InPunchTime)>=7
        AND datepart(hour, tc.InPunchTime)<=10
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3







    )
    UNION
    (Select 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        tc.OutPunchTime as 'Shift End',
                    'E103' as 'Home',
                    'Teena' as 'Manager',
                    'Kent County' as 'County'


        FROM [Attendance2022] atn

            left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)>='14:00' AND isp.[duration]>120)
        AND (cast(isp.begin_time as time)<='17:00' AND isp.[duration]>120))

        Left Join TimeCards2022 tc 
        ON atn.date=tc.InPunchDay
            AND (tc.EarnCode = 'R' OR tc.EarnCode is null)
                AND tc.Department='W103'

        WHERE atn.individual like 'JAMES%'
               AND isp.isp_program is NULL


    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        cast(tc.InPunchTime as time)>='14:00'
        AND cast(tc.InPunchTime as time)<='17:00'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3





    )
    UNION
    (Select 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        '11:59 PM' as 'Shift End',
                    'E103' as 'Home',
                    'Teena' as 'Manager',
                    'Kent County' as 'County'

        FROM [attendance2022] atn

             left Join isp
                ON (atn.date=isp.date)
                AND (isp.Individual=atn.individual)
                AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>=30)
                    AND (cast(isp.begin_time as time)<='11:59pm' AND isp.[duration]>=30))

        Left Join TimeCards2022 tc
            ON (atn.date=tc.InPunchDay)
                AND tc.Department='W103'

        WHERE atn.individual like 'JAMES%'
                AND isp.isp_program is NULL

    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        cast(tc.InPunchTime as time)>='6pm'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3





    )
    UNION
    (Select 
        atn.individual as 'Name',
        tc.OutPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        '12:00 AM' as 'Shift Start', 
        tc.OutPunchTime as 'Shift End',  
                    'E103' as 'Home',
                    'Teena' as 'Manager',
                    'Kent County' as 'County'

        FROM [attendance2022] atn

               left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)='12am' AND isp.[duration]>=30))

        Left Join TimeCards2022 tc
        ON (atn.date=tc.OutPunchDay)
                AND tc.Department='W103'

        WHERE atn.individual like 'JAMES%'
               AND isp.isp_program is NULL


    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay


    HAVING 
        cast(tc.InPunchTime as time)>='6pm'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3







    )

    -- CHRISTINA CHITUCK --

    UNION
    (Select 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        tc.OutPunchTime as 'Shift End',
                    'E103' as 'Home',
                    'Teena' as 'Manager',
                    'Kent County' as 'County'
        FROM [Attendance2022] atn

                left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)>='07:00' AND isp.[duration]>120)
            AND (cast(isp.begin_time as time)<='09:00' AND isp.[duration]>120))


        Left Join TimeCards2022 tc 
            ON atn.date=tc.InPunchDay
            AND (tc.EarnCode = 'R' OR tc.EarnCode is null)
                AND tc.Department='W103'

        WHERE atn.individual like 'CHIT%'
                AND isp.isp_program is NULL
                 -- IF THE INDIVIDUAL GOES TO DAY PROGRAM --
                AND (datepart(weekday,atn.date)<2 OR datepart(weekday,atn.date)>6) 
    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        datepart(hour, tc.InPunchTime)>=7
        AND datepart(hour, tc.InPunchTime)<=10
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3







    )
    UNION
    (Select 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        tc.OutPunchTime as 'Shift End',
                    'E103' as 'Home',
                    'Teena' as 'Manager',
                    'Kent County' as 'County'


        FROM [Attendance2022] atn

            left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)>='14:00' AND isp.[duration]>120)
        AND (cast(isp.begin_time as time)<='17:00' AND isp.[duration]>120))

        Left Join TimeCards2022 tc 
        ON atn.date=tc.InPunchDay
            AND (tc.EarnCode = 'R' OR tc.EarnCode is null)
                AND tc.Department='W103'

        WHERE atn.individual like 'CHIT%'
               AND isp.isp_program is NULL


    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        cast(tc.InPunchTime as time)>='14:00'
        AND cast(tc.InPunchTime as time)<='17:00'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3





    )
    UNION
    (Select 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        '11:59 PM' as 'Shift End',
                    'E103' as 'Home',
                    'Teena' as 'Manager',
                    'Kent County' as 'County'

        FROM [attendance2022] atn

             left Join isp
                ON (atn.date=isp.date)
                AND (isp.Individual=atn.individual)
                AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>=30)
                    AND (cast(isp.begin_time as time)<='11:59pm' AND isp.[duration]>=30))

        Left Join TimeCards2022 tc
            ON (atn.date=tc.InPunchDay)
                AND tc.Department='W103'

        WHERE atn.individual like 'CHIT%'
                AND isp.isp_program is NULL

    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        cast(tc.InPunchTime as time)>='6pm'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3





    )
    UNION
    (Select 
        atn.individual as 'Name',
        tc.OutPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        '12:00 AM' as 'Shift Start', 
        tc.OutPunchTime as 'Shift End',  
                    'E103' as 'Home',
                    'Teena' as 'Manager',
                    'Kent County' as 'County'

        FROM [attendance2022] atn

               left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)='12am' AND isp.[duration]>=30))

        Left Join TimeCards2022 tc
        ON (atn.date=tc.OutPunchDay)
                AND tc.Department='W103'

        WHERE atn.individual like 'CHIT%'
               AND isp.isp_program is NULL


    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay


    HAVING 
        cast(tc.InPunchTime as time)>='6pm'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3







    )

    -- BRIANNA WOOTERS --
    UNION
    (Select 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        tc.OutPunchTime as 'Shift End',
        'E103' as 'Home',
        'Teena' as 'Manager',
        'Kent County' as 'County'

    FROM [attendance2022] atn

        left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)>='07:00' AND isp.[duration]>120)
        AND (cast(isp.begin_time as time)<='09:00' AND isp.[duration]>120))


        Left Join TimeCards2022 tc 
        ON atn.date=tc.InPunchDay
            AND (tc.EarnCode = 'R' OR tc.EarnCode is null)
        AND tc.Department='W103'

    WHERE 
        atn.individual like 'WOOT%'
        AND isp.isp_program is NULL
        -- IF THE INDIVIDUAL GOES TO DAY PROGRAM --
        -- AND (datepart(weekday,atn.date)<2 OR datepart(weekday,atn.date)>6) 


    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        datepart(hour, tc.InPunchTime)>=7
        AND datepart(hour, tc.InPunchTime)<=10
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3




    )

    -- RALPH WRIGHT -- 

    UNION
    (Select 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        tc.OutPunchTime as 'Shift End',
                    'E104' AS 'Home',
                    'Teena' as 'Manager',
                    'Kent County' as 'County'

        FROM [attendance2022] atn


                left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)>='07:00' AND isp.[duration]>120)
            AND (cast(isp.begin_time as time)<='09:00' AND isp.[duration]>120))


        Left Join TimeCards2022 tc 
            ON atn.date=tc.InPunchDay
            AND (tc.EarnCode = 'R' OR tc.EarnCode is null)
                AND tc.Department='W104'

        WHERE atn.individual like 'WRIGHT%'
                AND isp.isp_program is NULL
                 -- IF THE INDIVIDUAL GOES TO DAY PROGRAM --
               and (datepart(weekday,atn.date)<2 OR datepart(weekday,atn.date)>6) 

    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        datepart(hour, tc.InPunchTime)>=7
        AND datepart(hour, tc.InPunchTime)<=10
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3






    )
    UNION
    (SELECT 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        tc.OutPunchTime as 'Shift End',
                    'E104' AS 'Home',
                    'Teena' as 'Manager',
                    'Kent County' as 'County'

        FROM [attendance2022] atn

                 left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)>='14:00' AND isp.[duration]>120)
        AND (cast(isp.begin_time as time)<='17:00' AND isp.[duration]>120))

        Left Join TimeCards2022 tc 
        ON atn.date=tc.InPunchDay
            AND (tc.EarnCode = 'R' OR tc.EarnCode is null)
                AND tc.Department='W104'

        WHERE atn.individual like 'WRIGHT%'
                AND isp.isp_program is NULL

    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        cast(tc.InPunchTime as time)>='14:00'
        AND cast(tc.InPunchTime as time)<='17:00'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3






    )
    UNION
    (Select 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        '11:59 PM' as 'Shift End',
                    'E104' AS 'Home',
                    'Teena' as 'Manager',
                    'Kent County' as 'County'

        FROM [attendance2022] atn

        left Join isp
                ON (atn.date=isp.date)
                AND (isp.Individual=atn.individual)
                AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>=30)
                    AND (cast(isp.begin_time as time)<='11:59pm' AND isp.[duration]>=30))

        Left Join TimeCards2022 tc
            ON (atn.date=tc.InPunchDay)
                AND tc.Department='W104'

        WHERE atn.individual like 'WRIGHT%'
               AND isp.isp_program is NULL

    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        cast(tc.InPunchTime as time)>='6pm'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3






    )
    UNION
    (Select 
        atn.individual as 'Name',
        tc.OutPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        '12:00 AM' as 'Shift Start', 
        tc.OutPunchTime as 'Shift End', 
                    'E104' AS 'Home',
                    'Teena' as 'Manager',
                    'Kent County' as 'County'
        FROM [attendance2022] atn

              left Join isp
        ON (atn.date=isp.date)
        AND (isp.Individual=atn.individual)
        AND ((cast(isp.begin_time as time)='12am' AND isp.[duration]>=30))

        Left Join TimeCards2022 tc
        ON (atn.date=tc.OutPunchDay)
                AND tc.Department='W104'

        WHERE atn.individual like 'WRIGHT%'
               AND isp.isp_program is NULL

    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay


    HAVING 
        cast(tc.InPunchTime as time)>='6pm'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3






    )

    UNION
    -- NYEA GOLDSBERRY --

    (Select 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        tc.OutPunchTime as 'Shift End',
        '8 Nairn' as 'Home', 
        'David' as 'Manager',
        'New Castle County' as 'County'

    FROM 
        [attendance2022] atn

        left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)>='07:00' AND isp.[duration]>120)
            AND (cast(isp.begin_time as time)<='10:00' AND isp.[duration]>120))

        Left Join TimeCards2022 tc 
            ON atn.date=tc.InPunchDay
            AND (tc.EarnCode = 'R' OR tc.EarnCode is null)
            AND tc.Department='SA8'


    WHERE 
        atn.individual like 'GOLDS%'
        AND isp.isp_program is NULL
        -- IF THE INDIVIDUAL GOES TO DAY PROGRAM --
        AND (datepart(weekday,atn.date)<2 OR datepart(weekday,atn.date)>6 
                AND atn.Date > '02/28/2023') 

    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        cast(tc.InPunchTime as time)>='07:00'
        AND cast(tc.InPunchTime as time)<='10:00'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3





    )

    UNION
    (Select 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        tc.OutPunchTime as 'Shift End',
        '8 Nairn' as 'Home', 
        'David' as 'Manager',
        'New Castle County' as 'County'


    FROM [attendance2022] atn

        left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)>='14:00' AND isp.[duration]>120)
            AND (cast(isp.begin_time as time)<='17:00' AND isp.[duration]>120))

        Left Join TimeCards2022 tc 
            ON atn.date=tc.InPunchDay
            AND (tc.EarnCode = 'R' OR tc.EarnCode is null)
            AND tc.Department='SA8'

    WHERE 
        atn.individual like 'GOLDS%'
        AND isp.isp_program is NULL


    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        cast(tc.InPunchTime as time)>='14:00'
        AND cast(tc.InPunchTime as time)<='17:00'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3





    )

    UNION
    (Select 
        atn.individual as 'Name',
        atn.Date as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        tc.InPunchTime as 'Shift Start', 
        '11:59 PM' as 'Shift End',
        '8 Nairn' as 'Home',
        'David' as 'Manager',
        'New Castle County' as 'County'

    FROM [attendance2022] atn

        left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)>='6pm' AND isp.[duration]>=30)
            AND (cast(isp.begin_time as time)<='11:30pm' AND isp.[duration]>=30))

        Left Join TimeCards2022 tc
            ON (atn.date=tc.InPunchDay)
            AND tc.Department='SA8'

    WHERE 
        atn.individual like 'GOLDS%'
        AND isp.isp_program is NULL

    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        cast(tc.InPunchTime as time)>='6pm'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3





    )

    UNION
    (Select 
        atn.individual as 'Name',
        tc.OutPunchDay as 'Date',
        concat(tc.firstname, ' ', tc.lastname) as 'Staff Name',
        datename(weekday, atn.date) as 'Weekday', 
        '12:00 AM' as 'Shift Start', 
        tc.OutPunchTime as 'Shift End',
        '8 Nairn' as 'Home', 
        'David' as 'Manager',
        'New Castle County' as 'County'

    FROM [attendance2022] atn

        left Join isp
            ON (atn.date=isp.date)
            AND (isp.Individual=atn.individual)
            AND ((cast(isp.begin_time as time)='12am' AND isp.[duration]>=30))

        Left Join TimeCards2022 tc
            ON (atn.date=tc.OutPunchDay)
            AND tc.Department='SA8'

    WHERE atn.individual like 'GOLDS%'
    AND isp.isp_program is NULL

    Group By atn.date, tc.EarnHours,

        atn.individual,
        tc.InPunchTime,
        tc.Firstname,
        tc.Lastname,
        tc.OutPunchTime, 
        tc.InPunchDay,
        atn.attendance,
        tc.OutPunchDay

    HAVING 
        cast(tc.InPunchTime as time)>='6pm'
          AND atn.attendance like '%12%'
        AND tc.EarnHours > 3


    )
    ORDER BY 1
    """
    missing_isp = pd.read_sql_query(mdq, con=engine)

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
    apt_issues = pd.read_sql_query(aiq, con=engine)

    def sh_utz(atn, tc):

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
        report['Department'] = report['Department'].replace(
            ['13B Dartmouth - Castlebrook (13B Dartmouth - Castlebrook)'],
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
        report = report[['Department', 'year', 'month', 'week', 'EarnHours', 'Approved Hours']]
        report['Utilization Rate'] = report['EarnHours'] / report['Approved Hours']

        return (report)

    sh_utz = sh_utz(atn, timecards)

    qa_df = qa_reports.qa_df

    # Data Excel File
    xlwriter = pd.ExcelWriter(fr"DataReport().xlsx")
    wb = xlwriter.book

    ## ISP DATA
    isp.to_excel(xlwriter, sheet_name="ISPs", index=False)
    apts.to_excel(xlwriter, sheet_name="Apts", index=False)
    sh_utz.to_excel(xlwriter, sheet_name="Wkly_Site_Utz", index=False)
    notion_ee.to_excel(xlwriter, sheet_name="Current_EE", index=False)
    qa_df.to_excel(xlwriter, sheet_name="QA Reports", index=False)
    xlwriter.close()
    report = xlwriter

    st.write(isp)

    return report

