import notion_df as nd
import pandas as pd
import datetime
nd.pandas()

notion_token = "secret_omL8nzIdOZySUeAtSOCHm0bUNh2ydXdohuePKPBXkxm"
qa_db_id = "b408c4f33b91448595c640f8d0b3dd21"
id_url = "https://www.notion.so/b408c4f33b91448595c640f8d0b3dd21?v=ca756db095b941b4981dbf0839b0de73&pvs=4"

qa_df = pd.DataFrame(nd.download(qa_db_id, api_key=notion_token, resolve_relation_values=True))

qa_df = qa_df[["Report Type",
               "Program",
               "Title",
               "Date of Review",
               "Initial Score",
               "Adjusted Compliance",
               "HM"]][qa_df["STATUS"] == "Finalized"]

qa_df["Program"] = qa_df["Program"].astype(str)
qa_df['Program'] = qa_df['Program'].replace(["['Broadstairs E103']"], 'Broadstairs E103')
qa_df['Program'] = qa_df['Program'].replace(["['8 Nairn Ln']"], '8 Nairn Ln')
qa_df['Program'] = qa_df['Program'].replace(["['3 Nairn Ln']"], '3 Nairn Ln')
qa_df['Program'] = qa_df['Program'].replace(["['13B Castlebrook']"], '13B Castlebrook')
qa_df['Program'] = qa_df['Program'].replace(["['Westover E104']"], 'Westover E104')
qa_df['Program'] = qa_df['Program'].replace(["['Katrina 110']"], 'Katrina 110')
qa_df['Program'] = qa_df['Program'].replace(["['Jeffrey 101']"], 'Jeffrey 101')






