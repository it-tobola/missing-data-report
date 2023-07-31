import notion_df as nd
import pandas as pd
import notion_db
nd.pandas()

# authorizations database that houses approved service hours
notion_key = notion_db.key
auths_db_id = notion_db.auths
o_auths = nd.download(auths_db_id, api_key=notion_key, resolve_relation_values=True)


# connection to the clients database
individuals_db_id = notion_db.service_recipients
clients = nd.download(individuals_db_id, api_key=notion_key, resolve_relation_values=True)
clients = clients[["MCI#", "Program"]]
clients["Program"] = clients["Program"]
# cleaning column values in clients database
for i, row in clients.iterrows():
    row["Program"] = row["Program"]
clients["MCI#"] = clients["MCI#"].astype('int')

# Creating a single dataframe to house the clients and their current service authorizations
auths = o_auths.merge(clients, left_on='ID', right_on='MCI#')

# cleaning column values
auths = auths.drop(["MCI#_x", "Program_x", "ID"], axis=1)
auths = pd.DataFrame(auths)
auths["Program"] = auths["Program_y"]


# Grouping clients by the program they are enrolled, and finding the sum of
# daily service hours approved for each program
program_auths = auths.groupby("Program")["Total SH"].sum()
program_auths = program_auths.reset_index()

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

def utz(timecards_df):

    timecards_df['InPunchDay'] = pd.to_datetime(timecards_df['InPunchDay'])

    start_date = timecards_df['InPunchDay'].min()
    end_date = timecards_df['InPunchDay'].max()
    dates = pd.date_range(start_date, end_date, freq='D')

    results = []
    for date in dates:
        # Filter the dataframe for authorizations active on the current day
        active_authorizations = auths[(auths['Auth Start'] <= date) & (auths['Auth End'] >= date)]

        # Calculate the total SH for each program on the current day
        active_authorizations["SH."] = active_authorizations["Total SH"].astype('float')
        total_sh_by_program = active_authorizations.groupby('Program')['Total SH'].sum()

        # Store the results for the current day
        results.append(total_sh_by_program)

    # Step 5: Create a new dataframe to store the results
    approved_sh = pd.DataFrame(results, index=dates)

    tc = timecards_df[['Department', 'InPunchDay', 'EarnHours']]

    day_totals = tc.groupby(['Department', 'InPunchDay'])['EarnHours'].sum().reset_index()
    day_report = pd.merge(tc, day_totals, on=['Department', 'InPunchDay'], suffixes=('', '_total'))

    day_report = day_report[['Department', 'InPunchDay', 'EarnHours_total']]
    day_report = day_report.drop_duplicates()
    day_report = day_report[day_report['Department'] != 'Administration']
    day_report = day_report.reset_index()

    daily_report = pd.DataFrame(day_report.groupby(['Department', "InPunchDay"])['EarnHours_total'].sum().reset_index())

    # Reset the index to convert the date index to a column
    approved_sh = approved_sh.reset_index()
    approved_sh.columns = ['Date', '13B Castlebrook', '3 Nairn Ln', '8 Nairn Ln',
       'Broadstairs E103', 'Jeffrey 101', 'Katrina 110', 'Westover E104']

    approved_sh = approved_sh.reset_index()

    # Reshape the dataframe using the melt() function
    approved_sh = approved_sh.melt(id_vars='Date', var_name='Department', value_name='SH')
    approved_sh = approved_sh[approved_sh["Department"] != 'index']

    for i, row in approved_sh.iterrows():
        approved_sh['Department'][approved_sh['Department'] == row['Department']] = d_codes[row["Department"]]


    report = pd.merge(approved_sh, daily_report, left_on=['Department', 'Date'], right_on=['Department', 'InPunchDay'])
    report = report.drop(columns=['InPunchDay'])
    report['UTZ'] = report["EarnHours_total"]/report["SH"]

    # Print the transformed dataframe
    print(report)

    return report





