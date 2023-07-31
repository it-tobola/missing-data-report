import pandas as pd
import azure_cnxn as az
from sqlalchemy import create_engine


def start(atn1, atn2, atn3, save_path, date):
    # enter that paths to the files for each quarter (3 months) of attendance
    path_list = []
    q1_path = atn1
    q2_path = atn2
    try:
        q3_path = atn3
    except:
        q3_path = ""
    final_path = save_path
    path_list += [q1_path, q2_path, q3_path, final_path]


    # Create a frame list
    frame_list = []

    try:
        q1 = pd.DataFrame(pd.read_excel(fr"{q1_path}"))
        q2 = pd.DataFrame(pd.read_excel(fr"{q2_path}"))
        q3 = pd.DataFrame(pd.read_excel(fr"{q3_path}"))
        frame_list += [q1, q2, q3]
        ytd = pd.concat([q1, q2, q3], ignore_index=True)
        frame_list += [ytd]
    except:
        q1 = pd.DataFrame(pd.read_excel(fr"{q1_path}"))
        q2 = pd.DataFrame(pd.read_excel(fr"{q2_path}"))
        frame_list += [q1, q2]
        ytd = pd.concat([q1, q2], ignore_index=True)
        frame_list += [ytd]

    # Create dataframes for the past 6 months


    # Update Column Names
    for frame in frame_list:
        frame.columns = ['program_site',
                         'individual',
                         'MCI',
                         'date',
                         'attendance',
                         'time_zone']

    # Update necessary column datatypes/values
    for frame in frame_list:
        frame.date = pd.to_datetime(frame.date)

    # Data Cleaning (Individual Names)

    ## E103
    ytd['individual'] = ytd['individual'].replace(["James, Janet"], 'James, Janet M')
    ytd['individual'] = ytd['individual'].replace(["Chituck, Christina"], 'Chituck, Christina L')
    ytd['individual'] = ytd['individual'].replace(["Wooters, Brianna"], 'Wooters, Brianna E')
    ytd['individual'] = ytd['individual'].replace(["Isip, Anna"], 'Isip, Anna I')

    ## E104
    ytd['individual'] = ytd['individual'].replace(["Wright, Ralph"], 'Wright, Ralph W')
    ytd['individual'] = ytd['individual'].replace(["Seward, Robert"], 'Seward, Robert')

    ## J101
    ytd['individual'] = ytd['individual'].replace(["LeVan, Charles"], 'LeVan, Charles J')

    ## K110
    ytd['individual'] = ytd['individual'].replace(["GREEN, JOSEPH E"], 'GREEN, JOSEPH E E')

    ## 3NL
    ytd['individual'] = ytd['individual'].replace(["Gallagher, James"], 'Gallagher, James M')
    ytd['individual'] = ytd['individual'].replace(["Garrison, Christian"], 'Garrison, Christian')
    ytd['individual'] = ytd['individual'].replace(["Lanier, Daniel"], 'Lanier, Daniel L')

    ## 8NL
    ytd['individual'] = ytd['individual'].replace(["Jardon-Rosales, Dulce"], 'Jardon-Rosales, Dulce Y')
    ytd['individual'] = ytd['individual'].replace(["Goldsberry, Nyea"], 'Goldsberry, Nyea Nicole')
    ytd['individual'] = ytd['individual'].replace(["Weiss, Stephanie"], 'Weiss, Stephanie L')

    ## Castlebrook
    ytd['individual'] = ytd['individual'].replace(["Faust, Travis"], 'Faust, Travis A')
    ytd['individual'] = ytd['individual'].replace(["Headen, Deven"], 'Headen, Deven T')

    #### End

    ytd.drop_duplicates

    site_dict = {
        "13B Dartmouth - Castlebrook (13B Dartmouth - Castlebrook)": "13B Castlebrook",
        "8 Nairn Ln (8 Nairn Ln)": "SA8",
        "3 Nairn Ln (3 Nairn Ln)": "SA3",
        "Katrina 110 (Katrina 110)": "K110",
        "324 Broadstairs (Broadstairs E103)": "W103",
        "Westover E104 (Westover-324)": "W104",
        "Cannon Mills - 101 (Jeffery Pl CLA)": "J101"
    }
    for i, row in ytd.iterrows():
        s1 = row['program_site']
        ytd['program_site'][ytd['program_site']==s1] = site_dict[s1]

    ytd.to_excel(fr"{save_path}\Attendance({date}).xlsx")

    return(ytd)


def write_to_table(DataFrame):
    # The next steps are used to drop the previous tables from the TOBOLA server
    #   and then create a replacement from the new data pull

    atn = DataFrame

    ##  Create Table
    from sqlalchemy.engine import URL
    cnxn_url = URL.create("mssql+pyodbc", query={"odbc_connect": az.cnxn_string})
    engine = create_engine(cnxn_url)
    atn.to_sql("Attendance2022", engine, index=False, if_exists='replace')