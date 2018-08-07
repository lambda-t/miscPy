import redis
import json
import time
import json

r = redis.StrictRedis(host='localhost', port=6379, db=0)

pipe = r.pipeline(transaction=False)
#r.set('foo', 'bar')
n =0
s = time.time()
#while n <= 10000:
pipe.get("HiBtc:LTCBTC")
#pipe.get("Bittrex:BTC-XRP")

#pipe.get("Binance:bnbbtc")
#pipe.get("Binance:ethbtc")
#pipe.get("Binance:bnbeth")

res = pipe.execute()
print(res[0])
#print(json.loads(res[0]))


#j = r.get('Binance:bnbbtc')
#z = r.get('Binance:ethbtc')
#x = r.get('Binance:bnbeth')
n+= 1
#print(j)
#print(z)
#print(x)
#print(r)
e = time.time()
print(e -s )




"""
a = json.loads(j)
#print(type(a['b']),a['b'])
i = 0
for e in a['b']:
    i += 1
#print(i,a['U'],a['u'])
print(i,a)
"""