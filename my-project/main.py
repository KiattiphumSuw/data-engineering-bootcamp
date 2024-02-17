import pandas as pd
import requests
print("hello world")

d = {'a': 1, 'b': 2, 'c': 3}
ser = pd.Series(data=d, index=['a', 'b', 'c'])

print(ser)