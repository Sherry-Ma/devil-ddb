import random
import string
from math import ceil, log
import time
import datetime

def generate_val(vtype, domain_size):
    if vtype == "INT":
        # [0, domain_size - 1)
        return round(domain_size * random.random())
    elif vtype == "FLOAT":
        digits = ceil(log(domain_size, 10))
        return round(random.random(), digits)
    elif vtype == "VARCHAR":
        length = ceil(log(domain_size, 26 + 10))
        return ''.join(random.choices(string.ascii_uppercase + string.digits, k = length))
    # elif vtype == "DATETIME":
    #     return "\'"  + time.strftime("%Y-%m-%d", time.localtime(stime + random.random() * (etime - stime))) + "\'" 
    else:
        raise NotImplementedError

size = 101000
table = [[i] for i in list(range(size))]
for row in range(size):
    table[row].append(generate_val("INT", 200))
    table[row].append(generate_val("FLOAT", 1000))
    table[row].append(generate_val("VARCHAR", 10000))


with open(f"./tests/qo/create_table_{size}.sql", "w") as fsql:
    for row in table:
        fsql.write(str(tuple(row)) + ',')
        fsql.write('\n')
    fsql.write(';')
# for row in table:
#     print(f"({row[0]}, {row[1]}, {row[2]}, {row[3]})")