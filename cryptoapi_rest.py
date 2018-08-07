import requests
import pandas as pd
import time

primary_coin = "USDT"

url = 'https://rest.coinapi.io/v1/exchangerate/' + primary_coin
#url = 'https://rest.coinapi.io/v1/exchanges'
header = {'X-CoinAPI-Key' : '82571109-8438-418D-A943-B1BA64581FD7'}
df = pd.DataFrame()
i = 0
res = requests.get(url, headers=header)
j = res.json()
print(j)

def get_btc_exchange(url,header,df,primary_coin):
    res = requests.get(url, headers=header)
    j = res.json()
    #print(j)

    df2 = pd.DataFrame(j["rates"])
    print(primary_coin)
    df2['primary_coin'] = primary_coin
    df = pd.concat([df,df2], axis = 0)
    with open("crypto_exchange_rates.csv", 'a') as f:
        df.to_csv(f, index=False)

def main(i,primary_coin):
    while True:
        time.sleep(5)
        get_btc_exchange(url,header,df,primary_coin)
        i += i +1
        print(i)

#main(i,primary_coin)