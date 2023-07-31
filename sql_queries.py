import pandas as pd
import notion_db
import notion_df as nd
import numpy as np
import datetime
nd.pandas()


def r_filter(atntable):
    token = notion_db.key

    # connection to authorizations notion df
    auths_id = notion_db.auths
    auths = pd.read_notion(auths_id, api_key=token, resolve_relation_values=True)

    # connection to programs dfs
    programs_db_id = notion_db.work_locations
    program_info = pd.read_notion(programs_db_id, api_key=token, resolve_relation_values=False)

    # grabbing client from the atn table
    clients = atntable[["individual", "MCI", "program_site"]].drop_duplicates()
    clients.columns = ['Name', 'MCI', 'Program']
    clients = clients.reset_index()

    # prep the columns for the filter
    report_filter = auths[['PA #',
                           'ID',
                           'Individual',
                           'Program',
                           'Day Shift',
                           'Evening Shift',
                           'Overnight Shift',
                           'Auth Start',
                           'Auth End']]
    report_filter["Manager"] = ''
    # Convert "ID" column to float type
    report_filter["ID"] = report_filter["ID"].astype(float)
    # Replace non-finite values with NaN
    report_filter["ID"].replace([np.inf, -np.inf], np.nan, inplace=True)

    # start the filter off with each authorization
    for i, row in report_filter.iterrows():
        id_1 = row.ID
        for r, bow in program_info.iterrows():
            id_list = []
            individuals = bow["Individuals"]
            if individuals:  # Check if the string is not empty
                id_list.extend(map(int, individuals.split(',')))
                if id_1 in id_list:
                    report_filter['Manager'][report_filter['ID'] == id_1] = bow['HM']
                    report_filter["Program"][report_filter['ID'] == id_1] = bow.Name



    # Dictionary connecting program values with d_code values
    d_codes = {
        "13B Castlebrook": "13B Castlebrook",
        "3 Nairn Ln": "SA3",
        "8 Nairn Ln": "SA8",
        "Broadstairs E103": "W103",
        "Jeffrey 101": "J101",
        "Katrina 110": "K110",
        "Westover E104": "W104"
    }
    d_codes = pd.DataFrame.from_dict(d_codes, orient="index").reset_index()
    d_codes.columns = ["Department", "d_code"]
    d_codes = d_codes.drop_duplicates()

    report_filter = pd.merge(left=report_filter, right=d_codes, left_on="Program", right_on="Department")

    return report_filter


def day_query(department, manager, client, authstart, authend):
    query = f"""
        SELECT
            atn.individual as 'Name',
            atn.Date as 'Date',
            CONCAT(tc.firstname, ' ', tc.lastname) as 'Staff Name',
            DATENAME(weekday, atn.date) as 'Weekday',
            tc.InPunchTime as 'Shift Start',
            tc.OutPunchTime as 'Shift End',
            '{department}' as 'Home',
            '{manager}' as 'Manager'
        FROM [Attendance2022] atn
        LEFT JOIN isp ON (atn.date = isp.date AND isp.Individual = atn.individual)
        LEFT JOIN TimeCards2022 tc ON atn.date = tc.InPunchDay AND (tc.EarnCode = 'R' OR tc.EarnCode IS NULL) AND tc.Department = '{department}'
        WHERE atn.individual = '{client}'
            AND isp.isp_program IS NULL
            AND (DATEPART(weekday, atn.date) >= 2 OR DATEPART(weekday, atn.date) <= 6)
            AND (atn.date >= '{authstart.strftime("%m/%d/%Y")}' OR atn.date IS NULL)
            AND (atn.date <= '{authend.strftime("%m/%d/%Y")}' OR atn.date IS NULL)
            AND ((DATEPART(hour, tc.InPunchTime) >= 7 AND DATEPART(hour, tc.InPunchTime) <= 11) AND atn.attendance LIKE '%P%' AND tc.EarnHours > 3)
    """
    return query


def eve_query(department, manager, client, authstart, authend):
    query = f"""
        SELECT
            atn.individual as 'Name',
            atn.Date as 'Date',
            CONCAT(tc.firstname, ' ', tc.lastname) as 'Staff Name',
            DATENAME(weekday, atn.date) as 'Weekday',
            tc.InPunchTime as 'Shift Start',
            tc.OutPunchTime as 'Shift End',
            '{department}' as 'Home',
            '{manager}' as 'Manager'
        FROM [Attendance2022] atn
        LEFT JOIN isp ON (atn.date = isp.date AND isp.Individual = atn.individual)
        LEFT JOIN TimeCards2022 tc ON atn.date = tc.InPunchDay AND (tc.EarnCode = 'R' OR tc.EarnCode IS NULL) AND tc.Department = '{department}'
        WHERE atn.individual = '{client}'
            AND isp.isp_program IS NULL
            AND (atn.date >= '{authstart.strftime("%m/%d/%Y")}' OR atn.date IS NULL)
            AND (atn.date <= '{authend.strftime("%m/%d/%Y")}' OR atn.date IS NULL)
            AND ((DATEPART(weekday, atn.date) >= 2 AND DATEPART(weekday, atn.date) <= 6)            
            AND (CAST(tc.InPunchTime AS time) >= '14:00' AND CAST(tc.InPunchTime AS time) <= '17:00' AND atn.attendance LIKE '%P%' AND tc.EarnHours > 3)
            OR
            (DATEPART(weekday, atn.date) = 1 OR DATEPART(weekday, atn.date) = 7)            
            AND (CAST(tc.InPunchTime AS time) >= '06:00' AND CAST(tc.InPunchTime AS time) <= '12:00' AND atn.attendance LIKE '%P%' AND tc.EarnHours > 3))
                   
"""
    return query


def night_query(department, manager, client, authstart, authend):
    query = f"""
        SELECT
            atn.individual as 'Name',
            atn.Date as 'Date',
            CONCAT(tc.firstname, ' ', tc.lastname) as 'Staff Name',
            DATENAME(weekday, atn.date) as 'Weekday',
            tc.InPunchTime as 'Shift Start',
            tc.OutPunchTime as 'Shift End',
            '{department}' as 'Home',
            '{manager}' as 'Manager'
        FROM [Attendance2022] atn
        LEFT JOIN isp ON (atn.date = isp.date AND isp.Individual = atn.individual)
        LEFT JOIN TimeCards2022 tc ON atn.date = tc.InPunchDay AND (tc.EarnCode = 'R' OR tc.EarnCode IS NULL) AND tc.Department = '{department}'
        WHERE atn.individual = '{client}'
            AND isp.isp_program IS NULL
            AND (atn.date >= '{authstart.strftime("%m/%d/%Y")}' OR atn.date IS NULL)
            AND (atn.date <= '{authend.strftime("%m/%d/%Y")}' OR atn.date IS NULL)
            AND (CAST(tc.InPunchTime AS time) >= '20:00' AND CAST(tc.InPunchTime AS time) <= '23:59' AND atn.attendance LIKE '%P%' AND tc.EarnHours > 3)
    """
    return query


def mdq(filters):
    import azure_cnxn as az

    report_filter = pd.DataFrame(columns=filters.columns)

    queries = []
    for i, row in filters.iterrows():
        department = row["d_code"]
        manager = row["Manager"]
        client = row["Individual"]
        day_shift = row["Day Shift"]
        eve_shift = row["Evening Shift"]
        night_shift = row["Overnight Shift"]
        authstart = pd.to_datetime(row["Auth Start"])  # Convert to datetime.datetime directly
        authend = pd.to_datetime(row["Auth End"])      # Convert to datetime.datetime directly

        if day_shift:
            query = day_query(department, manager, client, authstart, authend)
            queries.append(query)
        elif eve_shift:
            query = eve_query(department, manager, client, authstart, authend)
            queries.append(query)
        elif night_shift:
            query = night_query(department, manager, client, authstart, authend)
            queries.append(query)

    report = pd.DataFrame()
    engine = az.engine

    for query in queries:
        print("Executing query:")
        print(query)
        missing_isps = pd.read_sql_query(query, con=engine)
        report = pd.DataFrame(report.append(missing_isps))

    print("MISSING ISP'S")
    print(report)

    return report
