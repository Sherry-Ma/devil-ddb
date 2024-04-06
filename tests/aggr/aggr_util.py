import decimal
import math
import random
import string
from math import ceil, log
import time
import datetime
from copy import deepcopy
import psycopg2

# change the following config if necessary
DB_NAME = "ubuntu"
DB_USER = "ubuntu"
DB_PASS = "ubuntu"
DB_HOST = "localhost"
DB_PORT = "5432"
conn = psycopg2.connect(database=DB_NAME, user=DB_USER,
                        password=DB_PASS, host=DB_HOST,
                        port=DB_PORT)

supported_types = [
    "INT",
    "FLOAT", 
    "VARCHAR", 
    "DATETIME"
]
supported_aggrs = [
    "SUM",
    "COUNT",
    "AVG",
    "MIN",
    "MAX",
    "STDDEV_POP"
]
type_aggrs = {
    "INT": ["SUM", "COUNT", "AVG", "MIN", "MAX", "STDDEV_POP"],
    "FLOAT": ["SUM", "COUNT", "AVG", "MIN", "MAX", "STDDEV_POP"],
    "VARCHAR": ["COUNT", "MIN", "MAX"],
    "DATETIME": ["COUNT", "MIN", "MAX"],
}
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
    
def generate_special_table(scale: int, num_groups: int=10):
    """Schema: R(A: int, B: int, C: float, D: varchar, E: date)
    Group is indicated by A.
    Guarantee that the first group has scale // num_groups tuples.
    """
    table = []
    column_types = ['INT','INT','FLOAT','VARCHAR','DATETIME']
    if scale == 0:
        return table, column_types
    for i in range(num_groups):
        cnt = random.randint(1, scale // num_groups) if i > 0 else scale // num_groups
        for j in range(cnt):
            if j == 0:
                table.append([i] + [generate_val(supported_types[t], scale) for t in range(len(supported_types))])
                continue
            tmp_row = [i]
            for t in range(len(supported_types)):
                # flip a coin to determine if use last value
                if random.randint(0, 1) == 0:
                    tmp_row.append(generate_val(supported_types[t], scale))
                else:
                    tmp_row.append(table[-1][t + 1])
            table.append(tmp_row)
    # fill up the table to scale
    while len(table) < scale:
        table.append(table[-1])
    return table, column_types

def generate_simple_table(scale: int):
    """Generate special table for testing compound GB expressions.
    Schema R(A: int, B: int, C: int, D: float, E: varchar, F: varchar)
    """
    table = []
    column_types = ['INT','INT','INT','FLOAT','VARCHAR','VARCHAR']
    if scale == 0:
        return table, column_types
    varchar_domain = [generate_val('VARCHAR',scale) for i in range(5)]
    for i in range(scale):
        row = [i, random.randint(0,10), random.randint(0,10), generate_val('FLOAT',scale), varchar_domain[random.randint(0,4)], generate_val('VARCHAR', scale)]
        table.append(row)
    return table, column_types

def write_table_to_db(table: list[list[any]], column_types: list[str], table_name='R'):
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.commit()
    create_sql = f"CREATE TABLE {table_name}(" + ', '.join([f"{chr(ord('A') + i)} {column_types[i] if column_types[i] != 'DATETIME' else 'DATE'}" for i in range(len(column_types))]) + ")"
    cur.execute(create_sql)
    conn.commit()
    for row in table:
        cur.execute(f"INSERT INTO {table_name}({','.join([chr(ord('A') + i) for i in range(len(column_types))])}) VALUES ({','.join(['%s' for i in range(len(column_types))])})", tuple(row))
    conn.commit()
    cur.close()
    return

# test the correctness of a single (distinct) aggr in SELECT
def single_aggr_test(test_id, aggr_func, num_groups, scale): 
    """Generate test to see if aggregation on a single column (with DISTINCT) works fine.
    Schema: # R(A: int, B: int, C: float, D:varchar, E: date)
    Query: SELECT A, [aggr, aggr(DISTINCT) on each column] FROM R GROUP BY A
    """
    sql_filename = f"t_aggr_{test_id}.sql"
    ans_filename = f"t_aggr_{test_id}.ans"

    table, column_types = generate_special_table(scale, num_groups)
    shuffled_table = deepcopy(table)
    random.shuffle(shuffled_table)

    with open(sql_filename, "w") as fsql, open(ans_filename, "w") as fans:
        fsql.write("SET AUTOCOMMIT OFF;\n")
        fans.write("(SET, None)\n")
        fsql.write("CREATE TABLE R(" + ", ".join([f"{chr(ord('A') + col_i)} {column_types[col_i]}" for col_i in range(len(column_types))]) + ");\n")
        fans.write("(CREATE TABLE, None)\n")
        fsql.write("INSERT INTO R VALUES\n" + ",\n".join(['\t(' + ', '.join([str(shuffled_table[row_i][col_i]) for col_i in range(len(column_types))]) + ')' for row_i in range(scale)]) + ";\n")
        fans.write("(INSERT, None)\n")
        query = "SELECT A, "
        for i in range(1, len(column_types)):
            # for func in type_aggrs[column_types[i]]:
            if aggr_func in type_aggrs[column_types[i]]:
                query += f'{aggr_func}({chr(ord("A") + i)}), {aggr_func}(DISTINCT {chr(ord("A") + i)}), '
        query = query[:-2]
        query += " FROM R GROUP BY A;\n"
        fsql.write(query)
        fans.write(f"(SELECT, {num_groups})\n")

        # writing out answer
        j = 0
        for i in range(num_groups):
            tmp_table = []
            while j < len(table) and table[j][0] == i:
                tmp_table.append(table[j])
                j += 1
            transpose_table = [[tmp_table[j][i] for j in range(len(tmp_table))] for i in range(len(tmp_table[0]))]
            res = f'({str(i)}, '
            for k in range(1, len(column_types)):
                if aggr_func in type_aggrs[column_types[k]]:
                    if aggr_func == "SUM":
                        res += f'{str(sum(transpose_table[k]))}, {str(sum(set(transpose_table[k])))}, '
                    elif aggr_func == "COUNT":
                        res += f'{str(len(transpose_table[k]))}, {str(len(set(transpose_table[k])))}, '
                    elif aggr_func == "AVG":
                        res += f'{str(sum(transpose_table[k]) / len(transpose_table[k]))}, {str(sum(set(transpose_table[k])) / len(set(transpose_table[k])))}, '
                    elif aggr_func == "MIN":
                        res += f'{str(min(transpose_table[k]))}, {str(min(set(transpose_table[k])))}, '
                    elif aggr_func == "MAX":
                        res += f'{str(max(transpose_table[k]))}, {str(max(set(transpose_table[k])))}, '
                    elif aggr_func == "STDDEV_POP":
                        mean = sum(transpose_table[k]) / len(transpose_table[k])
                        set_mean = sum(set(transpose_table[k])) / len(set(transpose_table[k]))
                        res += f'{str(math.sqrt(sum(pow(x-mean,2) for x in transpose_table[k]) / len(transpose_table[k])))}, {str(math.sqrt(sum(pow(x-set_mean,2) for x in set(transpose_table[k])) / len(set(transpose_table[k]))))}, '
            res = res[:-2] + ")\n"
            fans.write(res)
        fsql.write("ROLLBACK;\n")
        fans.write("(ROLLBACK, None)\n")

def compound_aggr_test(test_id: int, num_groups: int, scale: int, compound_aggr_exprs: list[str]):
    """Generate test to see if compound aggre expr works fine.
    Schema: # R(A: int, B: int, C: float, D:varchar, E: date)
    Query: SELECT A, [compound_aggr_exprs] FROM R GROUP BY A
    """
    sql_filename = f"t_aggr_{test_id}.sql"
    ans_filename = f"t_aggr_{test_id}.ans"

    table, column_types = generate_special_table(scale, num_groups)
    shuffled_table = deepcopy(table)
    random.shuffle(shuffled_table)

    # store the data in database
    write_table_to_db(table, column_types)
    with open(sql_filename, "w") as fsql, open(ans_filename, "w") as fans:
        fsql.write("SET AUTOCOMMIT OFF;\n")
        fans.write("(SET, None)\n")
        fsql.write("CREATE TABLE R(" + ", ".join([f"{chr(ord('A') + col_i)} {column_types[col_i]}" for col_i in range(len(column_types))]) + ");\n")
        fans.write("(CREATE TABLE, None)\n")
        fsql.write("INSERT INTO R VALUES\n" + ",\n".join(['\t(' + ', '.join([str(shuffled_table[row_i][col_i]) for col_i in range(len(column_types))]) + ')' for row_i in range(scale)]) + ";\n")
        fans.write("(INSERT, None)\n")
        # query = "SELECT A, SUM(A + B) * 2, (SUM(A) + SUM(B)) * 2 - 3, AVG(B) - COUNT(DISTINCT E) + MAX(C) + 3, MIN(D) || '_hello'"
        query = f"SELECT A, {', '.join(compound_aggr_exprs)} FROM R GROUP BY A"
        cur = conn.cursor()
        cur.execute(query + " ORDER BY A")
        fsql.write(query + ";\n")
        fans.write(f"(SELECT, {cur.rowcount})\n")
        # writing out answer
        for row in cur.fetchall():
            res = "("
            for val in row:
                if isinstance(val, str):
                    res += "'" + str(val).replace("'", "") + "', "
                elif isinstance(val, decimal.Decimal):
                    res += f"{float(val)}, "
                elif isinstance(val, datetime.datetime):
                    res += "'" + str(val) + "', "
                else:
                    res += str(val) + ", "
            res = res[:-2]
            res += ")\n"
            fans.write(res)
        fsql.write("ROLLBACK;\n")
        fans.write("(ROLLBACK, None)\n")
        cur.close()

def group_expr_test(test_id: int, scale: int, group_exprs: list[str], select_exprs: list[str]):
    """Generate test just to test if compound group-by expr works fine.
    Schema: R(A: int, B: int, C: int, D: float, E: varchar, F: varchar)
    Query: SELECT [group_exprs], [some random aggregations] FROM R GROUP BY A 
    """
    sql_filename = f"t_aggr_{test_id}.sql"
    ans_filename = f"t_aggr_{test_id}.ans"

    table, column_types = generate_simple_table(scale)
    shuffled_table = deepcopy(table)
    random.shuffle(shuffled_table)

    # store the data in database
    write_table_to_db(table, column_types)

    with open(sql_filename, "w") as fsql, open(ans_filename, "w") as fans:
        fsql.write("SET AUTOCOMMIT OFF;\n")
        fans.write("(SET, None)\n")
        fsql.write("CREATE TABLE R(" + ", ".join([f"{chr(ord('A') + col_i)} {column_types[col_i]}" for col_i in range(len(column_types))]) + ");\n")
        fans.write("(CREATE TABLE, None)\n")
        fsql.write("INSERT INTO R VALUES\n" + ",\n".join(['\t(' + ', '.join([str(shuffled_table[row_i][col_i]) for col_i in range(len(column_types))]) + ')' for row_i in range(scale)]) + ";\n")
        fans.write("(INSERT, None)\n")
        # query = "SELECT A, SUM(A + B) * 2, (SUM(A) + SUM(B)) * 2 - 3, AVG(B) - COUNT(DISTINCT E) + MAX(C) + 3, MIN(D) || '_hello'"
        gb_str = ", ".join(group_exprs)
        query = f"SELECT {gb_str}, {', '.join(select_exprs)} FROM R GROUP BY {gb_str}"  # R(A: int, B: int, C: int, D: float, E: varchar, F: varchar)
        cur = conn.cursor()
        cur.execute(query + f" ORDER BY {gb_str}")
        fsql.write(query + ";\n")
        fans.write(f"(SELECT, {cur.rowcount})\n")
        # writing out answer
        for row in cur.fetchall():
            res = "("
            for val in row:
                if isinstance(val, str):
                    res += "'" + str(val).replace("'", "") + "', "
                elif isinstance(val, decimal.Decimal):
                    res += f"{float(val)}, "
                elif isinstance(val, datetime.datetime):
                    res += "'" + str(val) + "', "
                else:
                    res += str(val) + ", "
            res = res[:-2]
            res += ")\n"
            fans.write(res)
        fsql.write("ROLLBACK;\n")
        fans.write("(ROLLBACK, None)\n")
        cur.close()


# test the correctness of aggr predicates in HAVING
def having_expr_test(test_id: int, scale: int, select_exprs: list[str], having_conds: str):
    sql_filename = f"t_aggr_{test_id}.sql"
    ans_filename = f"t_aggr_{test_id}.ans"

    table, column_types = generate_special_table(scale)
    shuffled_table = deepcopy(table)
    random.shuffle(shuffled_table)

    # store the data in database
    write_table_to_db(table, column_types)
    with open(sql_filename, "w") as fsql, open(ans_filename, "w") as fans:
        fsql.write("SET AUTOCOMMIT OFF;\n")
        fans.write("(SET, None)\n")
        fsql.write("CREATE TABLE R(" + ", ".join([f"{chr(ord('A') + col_i)} {column_types[col_i]}" for col_i in range(len(column_types))]) + ");\n")
        fans.write("(CREATE TABLE, None)\n")
        fsql.write("INSERT INTO R VALUES\n" + ",\n".join(['\t(' + ', '.join([str(shuffled_table[row_i][col_i]) for col_i in range(len(column_types))]) + ')' for row_i in range(scale)]) + ";\n")
        fans.write("(INSERT, None)\n")
        query = f"SELECT A, {', '.join(select_exprs)} FROM R GROUP BY A HAVING {having_conds}"
        cur = conn.cursor()
        cur.execute(query + " ORDER BY A")
        fsql.write(query + ";\n")
        fans.write(f"(SELECT, {cur.rowcount})\n")
        # writing out answer
        for row in cur.fetchall():
            res = "("
            for val in row:
                if isinstance(val, str):
                    res += "'" + str(val).replace("'", "") + "', "
                elif isinstance(val, decimal.Decimal):
                    res += f"{float(val)}, "
                elif isinstance(val, datetime.datetime):
                    res += "'" + str(val) + "', "
                else:
                    res += str(val) + ", "
            res = res[:-2]
            res += ")\n"
            fans.write(res)
        fsql.write("ROLLBACK;\n")
        fans.write("(ROLLBACK, None)\n")
        cur.close()


def random_test(test_id: int, scale: int, query: str, group_expr_num: int):
    """Test against any random query over 2 tables (R and S).
    The query must be in the format of:
        SELECT [projected columns] FROM R, S [optional WHERE] GROUP BY [group-by exprs] [optional HAVING]
    The purpose is to test whether the implementation works in general.
    # R(A: int, B: int, C: float, D:varchar, E: date)
    # S(A: int, B: int, C: int, D: float, E: varchar, F: varchar)
    """
    sql_filename = f"t_aggr_{test_id}.sql"
    ans_filename = f"t_aggr_{test_id}.ans"

    rtable, r_column_types = generate_special_table(scale)
    stable, s_column_types = generate_simple_table(scale)
    write_table_to_db(rtable, r_column_types, 'R')
    write_table_to_db(stable, s_column_types, 'S')

    with open(sql_filename, "w") as fsql, open(ans_filename, "w") as fans:
        fsql.write("SET AUTOCOMMIT OFF;\n")
        fans.write("(SET, None)\n")
        # create R table
        fsql.write("CREATE TABLE R(" + ", ".join([f"{chr(ord('A') + col_i)} {r_column_types[col_i]}" for col_i in range(len(r_column_types))]) + ");\n")
        fans.write("(CREATE TABLE, None)\n")
        if rtable:
            fsql.write("INSERT INTO R VALUES\n" + ",\n".join(['\t(' + ', '.join([str(rtable[row_i][col_i]) for col_i in range(len(r_column_types))]) + ')' for row_i in range(scale)]) + ";\n")
            fans.write("(INSERT, None)\n")
        # create S table
        fsql.write("CREATE TABLE S(" + ", ".join([f"{chr(ord('A') + col_i)} {s_column_types[col_i]}" for col_i in range(len(s_column_types))]) + ");\n")
        fans.write("(CREATE TABLE, None)\n")
        if stable:
            fsql.write("INSERT INTO S VALUES\n" + ",\n".join(['\t(' + ', '.join([str(stable[row_i][col_i]) for col_i in range(len(s_column_types))]) + ')' for row_i in range(scale)]) + ";\n")
            fans.write("(INSERT, None)\n")
        # fetch and write answer
        cur = conn.cursor()
        cur.execute(query + f" ORDER BY {', '.join([str(i + 1) for i in range(group_expr_num)])}")
        fsql.write(query + ";\n")
        fans.write(f"(SELECT, {cur.rowcount})\n")
        for row in cur.fetchall():
            res = "("
            for val in row:
                if isinstance(val, str):
                    res += "'" + str(val).replace("'", "") + "', "
                elif isinstance(val, decimal.Decimal):
                    res += f"{float(val)}, "
                elif isinstance(val, datetime.datetime):
                    res += "'" + str(val) + "', "
                else:
                    res += str(val) + ", "
            res = res[:-2]
            res += ")\n"
            fans.write(res)
        fsql.write("ROLLBACK;\n")
        fans.write("(ROLLBACK, None)\n")
        cur.close()

if __name__ == "__main__":
    # Test aggregation function on a single column (0-4 on small scale, 5-9 on large scale)
    # Schema: R(A: int, B: int, C: float, D: varchar, E: datetime), group by A is default
    single_aggr_test(0, 'SUM', 10, 100)
    single_aggr_test(1, 'COUNT', 10, 100)
    single_aggr_test(2, 'AVG', 10, 100)
    single_aggr_test(3, 'MIN', 10, 100)
    single_aggr_test(4, 'MAX', 10, 100)
    single_aggr_test(5, 'STDDEV_POP', 10, 100)
    single_aggr_test(6, 'SUM', 4, 10000)
    single_aggr_test(7, 'COUNT', 4, 10000)
    single_aggr_test(8, 'AVG', 4, 10000)
    single_aggr_test(9, 'MIN', 4, 10000)
    single_aggr_test(10, 'MAX', 4, 10000)
    single_aggr_test(11, 'STDDEV_POP', 4, 10000)

    # Test compound aggregation expressions in select (10 on small scale, 11 on large scale)
    # Schema: R(A: int, B: int, C: float, D: varchar, E: datetime), group by A is default
    # It is weird that SUM(A - B) / COUNT(A - B) is always off by 1 compared with the ground truth. + is fine, splitting A,B into different expr also works
    compound_aggr_test(12, 10, 100, ["SUM(A + B) * 2", "(SUM(A) + SUM(B)) * 2 - 3", "AVG(B) - COUNT(DISTINCT E) + MAX(C) + 3", "MIN(D) || '_hello'"])
    compound_aggr_test(13, 5, 10000, ["(SUM(DISTINCT A) - SUM(DISTINCT B)) / (COUNT(DISTINCT A) - COUNT(DISTINCT B))", "AVG(DISTINCT A + B)", "4 * AVG(DISTINCT A - B) / 2", "A + AVG(C)"])

    # Test various group-by expressions along with different select expressions (12 on small scale, 13 on large scale)
    # Schema: R(A: int, B: int, C: int, D: float, E: varchar, F: varchar)
    group_expr_test(14, 100, group_exprs=["B + C", "E || '_hello'"], select_exprs=["AVG(DISTINCT B)", "SUM(C)", "MAX(D)", "MIN(E)", "COUNT(DISTINCT F), COUNT(1)"])
    group_expr_test(15, 10000, group_exprs=["A", "B - C"], select_exprs=["SUM(C + B) * 2", "AVG(B) - COUNT(DISTINCT E) + MAX(C) + 3", "MIN(E) || '_cs516'"])

    # Test HAVING filter (14 on small scale, 15 on large scale)
    # Schema: R(A: int, B: int, C: float, D: varchar, E: datetime), group by A is default
    having_expr_test(16, 100, select_exprs=["SUM(A+B)"], having_conds="SUM(A+B) > 300 AND COUNT(DISTINCT E) > 5")
    having_expr_test(17, 10000, select_exprs=["MAX(E)", "SUM(C)"], having_conds="MAX(E) > '2010-01-01' AND SUM(C) < 100")

    # Finally, throw in some random test (16-17 on medium scale)
    # Fixed schemas:
    # R(A: int, B: int, C: float, D:varchar, E: date)
    # S(A: int, B: int, C: int, D: float, E: varchar, F: varchar)
    # You can assume the FROM clause is simply R, S. Write any other clause as you like. But make sure group-by exprs are listed 
    # in the same order at the very beginning of SELECT clause (we use this to sort the result)
    random_test(18, 2000, "SELECT R.A, S.B - S.C, AVG(R.C + S.D) FROM R, S WHERE R.A = S.A GROUP BY R.A, S.B - S.C HAVING AVG(R.C + S.D) < 50.0", 2)
    random_test(19, 2000, "SELECT S.A, S.B + S.C, S.E, SUM(S.D * 2), COUNT(DISTINCT S.E) FROM R, S WHERE R.A = S.A AND R.B > S.B GROUP BY S.A, S.B + S.C, S.E HAVING SUM(S.D * 2) > 200 AND COUNT(DISTINCT S.E) = 1", 3)
    random_test(20, 0, "SELECT R.A, COUNT(1) FROM R, S GROUP BY R.A HAVING COUNT(1) > 1", 1)

    # make sure to clean database and close
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS R")
    cur.execute("DROP TABLE IF EXISTS S")
    conn.commit()
    cur.close()
    conn.close()