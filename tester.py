from urllib import request

url = "http://nflsavant.com/pbp_data.php?year=2021"

request.urlretrieve(url, "nfl_2021.csv")
