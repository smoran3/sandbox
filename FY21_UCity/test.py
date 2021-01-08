import os
import pandas as pd

a = [1,2,3]
b = ['d', 'e', 'f']
c = ['x', 'y', 'z']

df = pd.DataFrame(list(zip(a, b, c)),
    columns = ['route', 'allday', 'ampeak'])

print df

df.to_csv(r'D:\dvrpc_shared\Sandbox\FY21_UCity\%s.csv' %'testname', index=False)
