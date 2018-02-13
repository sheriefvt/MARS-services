__author__ = 'sherief'

import requests

url12 = "http://sfx1.vbi.vt.edu:38080/graphservice/measure/compute"

data11={'graph':'sample','measure':'1'}

r = requests.get(url12,params=data11)

print r.text