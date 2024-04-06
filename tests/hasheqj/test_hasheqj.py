import pytest
import datetime
import subprocess

from ddb.db import DatabaseManager
from ddb.session import Session
from ddb.parser import parse_all

testcase_dir = "tests/hasheqj/"
T = 5 # total number of test cases

@pytest.fixture
def session():
    dbm = DatabaseManager(
        db_dir = DatabaseManager.DEFAULT_DB_DIR,
        tmp_dir = DatabaseManager.DEFAULT_TMP_DIR
    )
    s = Session(dbm)
    yield s

def load_anaswer(t_id):
    ans_filename = f"t_hasheqj_{t_id}.ans"
    answer, join_answer = None, None
    with open(testcase_dir + ans_filename, "r") as fans:
        # From t_hasheqj_{t_id}, load commands' responses and join content to answer and join_answer respectively
        answer, join_answer = [], []
        lines = list(fans.readlines())
        i = 0
        while i < len(lines):
            line = lines[i]
            line = line.lstrip('(').rstrip(')\n')
            answer.append((str(line.split(", ")[0]), eval(line.split(", ")[1])))
            if answer[-1][0] == "SELECT":
                # load all the join content
                for j in range(answer[-1][1]):
                    line = lines[i + 1 + j]
                    join_answer.append(eval(line[:-1]))
                i = i + answer[-1][1]
            i += 1
    return answer, join_answer

@pytest.mark.parametrize("t_id", list(range(T)))
def test_session(session, capsys, t_id):
    subprocess.run(['make', 'clean'], check=True)
    with open(testcase_dir + f"t_hasheqj_{t_id}.sql") as fsql:
        answer, join_answer = load_anaswer(t_id)
        assert answer is not None and join_answer is not None, f"Test{t_id}: faild to load answer file"
        # Check command one by one
        command_id = 0
        for parse_tree in parse_all(fsql.read()):
            r = session.request(parse_tree)
            assert r.error is None, f"Test{t_id}, command{command_id}: got error."
            assert r.error_details is None, f"Test{t_id}, command{command_id}: got error message."
            assert r.response.startswith(answer[command_id][0]), f"Test{t_id}, command{command_id}: incorrect command."
            if r.response.startswith("SELECT"):
                # check the join count
                assert int(r.response.split("\n")[0].split(" ")[1]) == answer[command_id][1], f"Test{t_id}, command{command_id}: incorrect join count."
                # check the join content
                join_result = capsys.readouterr().out.split("\n")[1:-1]
                join_result = [list(eval(item, {'datetime' : datetime})) for item in join_result]
                for row_i in range(len(join_result)):
                    for col_i in range(len(join_result[row_i])):
                        if isinstance(join_result[row_i][col_i], datetime.datetime):
                            join_result[row_i][col_i] = join_result[row_i][col_i].strftime("%Y-%m-%d")
                    join_result[row_i] = tuple(join_result[row_i])
                join_result.sort()
                assert join_result == join_answer, f"Test{t_id}, command{command_id}: incorrect join content"
            command_id += 1
        # check if all commands are fully executed
        assert command_id == len(answer), f"Test{t_id}: executed {command_id} commands <> total {len(answer)} commands"


