import pytest
import datetime
import numpy as np
import subprocess


from ddb.db import DatabaseManager
from ddb.session import Session
from ddb.parser import parse_all

testcase_dir = "tests/qo/"
T = 4
times = 1

@pytest.fixture
def session():
    dbm = DatabaseManager(
        db_dir = DatabaseManager.DEFAULT_DB_DIR,
        tmp_dir = DatabaseManager.DEFAULT_TMP_DIR
    )
    s = Session(dbm)
    yield s


def helper(session, sql_file, times):
    latency = []
    for command_id, parse_tree in enumerate(parse_all(sql_file.read())):
        r = session.request(parse_tree)
        
        if r.r_pop is not None:
            # for _ in range(times):
            #     r = session.request(parse_tree)
            #     assert r.r_pop is not None, f"{_}, {r} {parse_tree}"
            latency.append(r.r_pop.measured.ns_elapsed.sum/1000000)
            est_io = r.r_pop.estimated_cost
            real_io = r.r_pop.measured.sum_blocks.overall
    return np.median(np.array(latency)), est_io, real_io


def create_table(session):
    # session.request("SET AUTOCOMMIT OFF;")
    file_name = ['create_table_100.sql', 'create_table_1000.sql', 'create_table_10000.sql', 'create_table_100000.sql']
    for file in file_name:
        create_table = open(testcase_dir + file)
        for command_id, parse_tree in enumerate(parse_all(create_table.read())):
            r = session.request(parse_tree)
            if r.error:
                break

@pytest.mark.parametrize("t_id", list(range(1, T+1)))
def test_session(session, capsys, t_id):
    subprocess.run(['make', 'clean'], check=True)
    create_table(session)
    fsql_qo =  open(testcase_dir + f"q{t_id}.sql")
    fsql_baseline = open(testcase_dir + f"q{t_id}-baseline.sql")
    avg_latency_qo, est_io_qo, real_io_qo = helper(session, fsql_qo, times)
    avg_latency_baseline, est_io_baseline, real_io_baseline = helper(session, fsql_baseline, times)
    query_result = capsys.readouterr().out.split("\n")
    print(f"Test{t_id}: {avg_latency_qo}ms, {avg_latency_baseline}ms")
    print(f"Test{t_id}: {real_io_qo} I/O {real_io_baseline} I/O")
    print(f"Test{t_id}: {est_io_qo} Est I/O {est_io_baseline} Est I/O")
    assert query_result[1] == query_result[3], f"Test{t_id} result is wrong: {query_result[1]}, should be {query_result[3]}"
    assert est_io_qo < est_io_baseline, f"Test{t_id}: {est_io_qo}I/O, {est_io_baseline}I/O"
    # assert avg_latency_qo < avg_latency_baseline, f"Test{t_id}: {avg_latency_qo}ms, {avg_latency_baseline}ms"
    # assert real_io_qo < real_io_baseline, f"Test{t_id}: {real_io_qo} I/O {real_io_baseline} I/O"
    

# poetry run pytest tests/qo/test_qo.py -rA --tb=line
