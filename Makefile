.PHONY:	clean check run test test-debug doc

clean:
	rm -rf alps.ddb
	rm -rf alps-tmp.ddb

check:
	(cd src; mypy ddb)

run:	clean
	python -m ddb.db

debug:	clean
	python -m ddb.db --debug

test:	clean
	pytest

test-debug:	clean
	pytest -s --log-cli-level=DEBUG

doc:
	(cd docs; make clean; rm -f source/_modules/*; sphinx-apidoc -aM -o ./source/_modules ../src/ddb)
	(cd docs; make html)
