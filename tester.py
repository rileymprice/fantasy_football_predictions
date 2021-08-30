import pandas as pd


positions = ["qb", "wr", "te", "rb"]
week = 1
for pos in positions:
    url = (
        f"https://www.fantasypros.com/nfl/projections/{pos}.php?scoring=PPR&week={week}"
    )
    data = pd.read_html(url)
    break
table = data[0]
print(table.info)
