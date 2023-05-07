import notion_df as nd
import pandas as pd
import datetime
nd.pandas()

key = "secret_omL8nzIdOZySUeAtSOCHm0bUNh2ydXdohuePKPBXkxm"

waiver_rates_url = "https://www.notion.so/6293fb00a938416686514a1950938e93?v=e24e4d58cb1a4220916c400cf1174680&pvs=4"
waiver_rates_id = "6293fb00a938416686514a1950938e93"
waiver_rates = pd.DataFrame(nd.download(waiver_rates_id, api_key=key))

waiver_rates["FY End"] = pd.to_datetime(waiver_rates["FY End"].values).date
waiver_rates["FY Start"] = pd.to_datetime(waiver_rates["FY Start"].values).date
waiver_rates.FY = pd.to_datetime(waiver_rates.FY).dt.year

individuals_url = "https://www.notion.so/63d18ada53dd4fe195684bfa4c1faf94?v=ca504406356e44ce8d981c15e5ca3751&pvs=4"
individuals_id = "63d18ada53dd4fe195684bfa4c1faf94"
individuals = pd.DataFrame(nd.download(individuals_id, api_key=key, resolve_relation_values=True))


def approved_hrs(auth_path, waiver_rates):
    auths = pd.read_excel(auth_path)

    auths = auths[["Individual",
                   "Begin Date",
                   "End Date",
                   "Unit Rate ($)"]]

    auths["Begin Date"] = pd.to_datetime(auths["Begin Date"]).dt.date
    auths["End Date"] = pd.to_datetime(auths["End Date"]).dt.date
    auths['FY'] = 0

    # FY - 22
    fy22 = auths[auths['Begin Date'] >= datetime.date(year=2021, month=7, day=1)]
    fy22 = fy22[fy22['End Date'] <= datetime.date(year=2022, month=6, day=30)]
    fy22["FY"] = 2022
    fy22 = pd.merge(fy22, waiver_rates, on="FY")
    fy22 = fy22[["Individual",
                 "Begin Date",
                 "End Date",
                 "Unit Rate ($)",
                 "FY",
                 "Residential Rate"]]
    fy22["Approved Hours"] = fy22['Unit Rate ($)'] / fy22['Residential Rate']

    # FY - 23
    fy23 = auths[auths['Begin Date'] >= datetime.date(year=2022, month=7, day=1)]
    fy23 = fy23[fy23['End Date'] <= datetime.date(year=2023, month=6, day=30)]
    fy23["FY"] = 2023
    fy23 = pd.merge(fy23, waiver_rates, on="FY")
    fy23 = fy23[["Individual",
                 "Begin Date",
                 "End Date",
                 "Unit Rate ($)",
                 "FY",
                 "Residential Rate"]]
    fy23["Approved Hours"] = fy23['Unit Rate ($)'] / fy23['Residential Rate']

    auths = pd.concat([fy22, fy23])

    # Clean up client names

    ## E103
    auths['Individual'] = auths['Individual'].replace(["James, Janet"], 'Janet M.')
    auths['Individual'] = auths['Individual'].replace(["Chituck, Christina"], 'Christina C.')
    auths['Individual'] = auths['Individual'].replace(["Wooters, Brianna"], 'Brianna W.')

    ## E104
    auths['Individual'] = auths['Individual'].replace(["Wright, Ralph"], 'Ralph W.')
    auths['Individual'] = auths['Individual'].replace(["Seward, Robert"], 'Robert S.')

    ## J101
    auths['Individual'] = auths['Individual'].replace(["LeVan, Charles"], 'Charles L.')

    ## K110
    auths['Individual'] = auths['Individual'].replace(["GREEN, JOSEPH E"], 'Joseph G.')

    ## 3NL
    auths['Individual'] = auths['Individual'].replace(["Gallagher, James"], 'James G.')
    auths['Individual'] = auths['Individual'].replace(["Garrison, Christian"], 'Christian G.')
    auths['Individual'] = auths['Individual'].replace(["Lanier, Daniel"], 'Daniel L.')

    ## 8NL
    auths['Individual'] = auths['Individual'].replace(["Jardon-Rosales, Dulce"], 'Dulce JR.')
    auths['Individual'] = auths['Individual'].replace(["Goldsberry, Nyea"], 'Nyea G.')

    ## Castlebrook
    auths['Individual'] = auths['Individual'].replace(["Faust, Travis"], 'Travis F.')
    auths['Individual'] = auths['Individual'].replace(["Headen, Deven"], 'Deven H.')

    auths['Program'] = ""
    for i, row in auths.iterrows():
        for name in individuals["Full Name"]:
            row['Program'] = individuals.Program[individuals['Full Name'] == row['Individual']].values

    print(auths)
    print(individuals["Full Name"])



auth_path = fr"C:\Users\olato\OneDrive\Desktop\TOBOLA QA REVIEW\Data_Pulls\2023\5_May\5.1.23\authorizations.xlsx"
atn = fr"C:\Users\olato\OneDrive\Desktop\TOBOLA QA REVIEW\Data_Pulls\2023\5_May\5.1.23\authorizations.xlsx"

approved_hrs(auth_path, waiver_rates)