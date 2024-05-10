# Devil's DataBase (`ddb`)

We assume you are in your course container shell. If you have a different setup, your mileage may vary: at the very least you will need `make` and Python (>= 3.11) with `poetry`.

## Getting Started

1. One (and only one) team member should fork this repo by clicking the small 'Fork' button at the very top right on GitLab.
   It's important that you fork first, because if you clone this repo directly you won't be able to push changes (save your progress) back to this repo (which is owned by the teaching staff).
   Name your forked repo as you prefer.
   In your newly forked repo, find the blue "Clone" button.
   Copy the "Clone with SSH" text.
   Add your teammates as members of your project as "Maintainers."
   Share the copied text with your teammates so they have access to this repo too.
   The remaining steps should be carried out by all team members.

2. In your container shell, issue the command git clone THE_TEXT_YOU_JUST_COPIED (make sure to replace THE_TEXT_YOU_JUST_COPIED with the "Clone with SSH" text).

3. In your container shell, change into the repository directory and then run `./install.sh`.
   This will mostly set up a `poetry` environment for you and install some dependencies.

## Running

* Before running, prepare the Python environment by running `poetry shell`.
  While inside the correct environment, you should see `(ddb-py3.11)` at the beginning of your command-line prompt.

* To run the `ddb` interpreter, issue the command `python -m ddb.db`.
  You know you are inside the `ddb` interpreter if you see the (blue) prompt `ddb> `.
  Besides standard SQL (we only support a small fragment, notably without subqueries), here are some useful commands:
  - `show tables;`
  - `set debug on;` (or `off`)
  - `set autocommit on;` (or `off`):
    The default is `off`, which commits every statement/command.
    But with this option on, you will be able to modify your database, play around with it,
    and `rollback;` to undo all the changes.
  - `analyze;`:
    Collect statistics on your tables and indexes.
  - <kbd>Ctrl</kbd>+<kbd>D</kbd>:
    Exit the `ddb` interpreter.

* Use `python -m ddb.db --help` to see various options for running `ddb`.
  In particular, the following is useful for running all statements in a `.sql` file as if they were typed in line by line:
  ```
  python -m ddb.db -i alps.sql
  ```

* `ddb` needs one directory to store data (defaults to `alps.db/`) and one as temp scratch space (defaults to `alps-tmp.ddb/`).
  To drop the database so you can start from a clean slate, simply remove these directories (in your container shell):
  ```
  \rm -rf alps.db/ alps-tmp.db/
  ```

## Developing and Debugging

* The source code uses Python compile-time type checking (`mypy` and `typing`) heavily.
  We highly recommend learning how to use Python type annotations --- you will be thankful how many bugs it can help you avoid.
  Whenever you have made non-trivial changes to code, use the command `make check` in your container shell to run the type checker.

* To run the unit test in `tests/`, use `make test` in your container shell.

* We generate documentation automatically from the source code.
  Run `make doc`, and then you will find the HTML documentation under `docs/build/html/index.html`,
  which you can then open and view using your web browser.

* For debugging, besides `print()`, we highly recommend using the VSCode + container setup
  (see Help section of the course website for details).
  Make sure the correct Python interpreter (the one in `.venv/` set up by `poetry`) is picked up by VSCode.
  The file `.vscode/launch.json` code repository sets up a debugging profile,
  which you can use to launch the VSCode debugger.
  The debugger allows you to set breakpoints, examine object contents, explore the call stack, etc.

## Directory Structure

* `src/`: Python source code for `ddb`.
   You will work mostly with files therein.

* `tests/`: Unit tests.
  We use `pytest` as the testing framework.

* `docs/`: Documentation.
  We use Sphinx to automatically generate documentation pages from comments in code.
  - `source/conf.py`: In case you want to tweak Sphinx behavior.
  - `build/html/index.html`: Home page of the automatically generated HTML documentation.

* `alps.ddb/` and `alps-tmp.ddb/`: Default database/tmp storage areas, respectively.
  The tmp directory will be cleared every time upon startup;
  the database directory will be preserved.

* `alps.sql`: Some statements for testing.
