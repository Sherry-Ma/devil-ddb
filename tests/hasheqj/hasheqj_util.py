import random
import string
from math import ceil, log
import time
import datetime


supported_types = [
    "INT",
    "FLOAT", 
    "VARCHAR", 
    "DATETIME"
]
min_datetime = '2000-01-01'
max_datetime = '2024-01-01'
stime = time.mktime(time.strptime(min_datetime, "%Y-%m-%d"))
etime = time.mktime(time.strptime(max_datetime, "%Y-%m-%d"))

def generate_val(vtype, domain_size):
    if vtype == "INT":
        # [0, domain_size - 1)
        return round(domain_size * random.random())
    elif vtype == "FLOAT":
        digits = ceil(log(domain_size, 10))
        return round(random.random(), digits)
    elif vtype == "VARCHAR":
        length = ceil(log(domain_size, 26 + 10))
        return "\'" + ''.join(random.choices(string.ascii_uppercase + string.digits, k = length)) + "\'" 
    elif vtype == "DATETIME":
        return "\'"  + time.strftime("%Y-%m-%d", time.localtime(stime + random.random() * (etime - stime))) + "\'" 
    else:
        raise NotImplementedError

# 0-based test_id
# l_size/r_size: a tuple (# of rows, # of cols)
# max_fanout: for one tuple on either side, can join with at most #max_fanout tuples on the other side
def random_test(test_id, l_size, r_size, max_fanout, random_seed=None):
    if random_seed is not None:
        random.seed(random_seed)

    assert l_size[0] >= r_size[0]
    assert l_size[1] + r_size[1] <= 26
    
    sql_filename = f"t_hasheqj_{test_id}.sql"
    ans_filename = f"t_hasheqj_{test_id}.ans"

    assert max_fanout >= 0 and max_fanout <= l_size[0] and max_fanout <= r_size[0]
    
    join_column_type = random.sample(supported_types, 1)
    l_column_type = join_column_type + random.sample(supported_types, l_size[1] - 1)
    r_column_type = join_column_type + random.sample(supported_types, r_size[1] - 1)

    l_table = []
    for _ in range(l_size[0]):
        l_table.append([None] + [generate_val(l_column_type[col_i], l_size[0]) for col_i in range(1, l_size[1])])
    r_table = []
    for _ in range(r_size[0]):
        r_table.append([None] + [generate_val(r_column_type[col_i], r_size[0]) for col_i in range(1, r_size[1])])
    
    all_joined_vals = {}
    cur_join_cnt = 0
    join_result = []
    l_id = 0
    r_id = 0
    while l_id < l_size[0] or r_id < r_size[0]:
        if l_id == l_size[0]:
            while r_id < r_size[0]:
                val = generate_val(r_column_type[0], r_size[0])
                while val in all_joined_vals:
                    val = generate_val(r_column_type[0], r_size[0])
                r_table[r_id][0] = val
                r_id += 1
            break
        if r_id == r_size[0]:
            while l_id < l_size[0]:
                val = generate_val(l_column_type[0], l_size[0])
                while val in all_joined_vals:
                    val = generate_val(l_column_type[0], l_size[0])
                l_table[l_id][0] = val
                l_id += 1
            break
        ln = random.randint(0, min(max_fanout, l_size[0] - l_id))
        rn = random.randint(0, min(max_fanout, r_size[0] - r_id))
        shared_val = generate_val(l_column_type[0], l_size[0])
        while shared_val in all_joined_vals:
            shared_val = generate_val(l_column_type[0], l_size[0])
        all_joined_vals[shared_val] = True
        cur_join_cnt += ln * rn
        for ll in range(l_id, l_id + ln):
            for rr in range(r_id, r_id + rn):
                join_result.append(tuple([shared_val] + l_table[ll][1:] + [shared_val] + r_table[rr][1:]))
        while ln > 0:
            l_table[l_id][0] = shared_val
            l_id += 1
            ln -= 1
        while rn > 0:
            r_table[r_id][0] = shared_val
            r_id += 1
            rn -= 1
    print(f"Test{test_id}: left_table = {l_size[0]}x{l_size[1]}, right_table = {r_size[0]}x{r_size[1]}, join_cnt = {cur_join_cnt}")
    random.shuffle(l_table)
    random.shuffle(r_table)
    join_result.sort()
    with open(sql_filename, "w") as fsql, open(ans_filename, "w") as fans:
        fsql.write("SET AUTOCOMMIT OFF;\n")
        fans.write("(SET, None)\n")
        fsql.write("SET INDEX_JOIN OFF;\n")
        fans.write("(SET, None)\n")
        fsql.write("SET SORT_MERGE_JOIN OFF;\n")
        fans.write("(SET, None)\n")
        fsql.write("CREATE TABLE R(" + ", ".join([f"{chr(ord('A') + col_i)} {l_column_type[col_i]}" for col_i in range(l_size[1])]) + ");\n")
        fans.write("(CREATE TABLE, None)\n")
        fsql.write("INSERT INTO R VALUES\n" + ",\n".join(['\t(' + ', '.join([str(l_table[row_i][col_i]) for col_i in range(l_size[1])]) + ')' for row_i in range(l_size[0])]) + ";\n")
        fans.write("(INSERT, None)\n")
        fsql.write("CREATE TABLE S(" + ", ".join([f"{chr(ord('A') + l_size[1] + col_i)} {r_column_type[col_i]}" for col_i in range(r_size[1])]) + ");\n")
        fans.write("(CREATE TABLE, None)\n")
        fsql.write("INSERT INTO S VALUES\n" + ",\n".join(['\t(' + ', '.join([str(r_table[row_i][col_i]) for col_i in range(r_size[1])]) + ')' for row_i in range(r_size[0])]) + ";\n")
        fans.write("(INSERT, None)\n")
        fsql.write("ANALYZE;\n")
        fans.write("(ANALYZE, None)\n")
        fsql.write(f"SELECT * FROM R, S WHERE R.A = S.{chr(ord('A') + l_size[1])};\n")
        fans.write(f"(SELECT, {cur_join_cnt})\n")
        for content in join_result:
            fans.write("(")
            for col_i in range(l_size[1] + r_size[1]):
                field_type = l_column_type[col_i] if col_i < l_size[1] else r_column_type[col_i - l_size[1]]
                fans.write(str(content[col_i]))
                if col_i < l_size[1] + r_size[1] - 1:
                    fans.write(", ")
            fans.write(")\n")
        fsql.write("ROLLBACK;\n")
        fans.write("(ROLLBACK, None)\n")


if __name__ == "__main__":
    # test_id, (#row0, #col0), (#row1, #col1), max_fanout, random_seed
    # make sure that #row0 >= row1 and #col0 + #col1 <= 26
    random_test(0, (8, 3), (6, 2), 2, 0)
    random_test(1, (20, 3), (15, 4), 5, 1)
    random_test(2, (100, 4), (88, 2), 40, 2)
    random_test(3, (1000, 2), (1000, 3), 200, 3)
    random_test(4, (10000, 4), (8000, 5), 100, 4)
    random_test(5, (100000, 2), (50000, 2), 50, 5)
    