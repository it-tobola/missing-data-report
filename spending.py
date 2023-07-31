import notion_df as nd
import pandas as pd
import notion_db
nd.pandas()

tx_id = notion_db.transactions
tx = pd.read_notion(tx_id, api_key=notion_db.key, resolve_relation_values=True)
tx['Month'] = pd.to_datetime(tx["Date"]).dt.month
tx['Year'] = pd.to_datetime(tx["Date"]).dt.year

# Transaction Categories
categories = []
for category in tx.Category:
    categories += [category]

budgets_id = notion_db.budgets
budgets = pd.read_notion(budgets_id, api_key=notion_db.key, resolve_relation_values=False)
print(budgets)

print(tx["Program"])