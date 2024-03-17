SET AUTOCOMMIT OFF;

CREATE TABLE R(A INT, B VARCHAR, C INT, PRIMARY KEY(A));
CREATE INDEX ON R(B);
INSERT INTO R VALUES
    (6, 'six', 12),
    (1, 'one', 2),
    (2, 'two', 4),
    (3, 'three', 6),
    (4, 'four', 8);

CREATE TABLE S(C INT, D VARCHAR, E FLOAT, F INT, G DATETIME);
CREATE INDEX ON S(C);
INSERT INTO S VALUES
    (2, 'S two', 2.0, 4, '2000-01-02'),
    (4, 'S four', 4.0, 8, '2000-01-04'),
    (6, 'S six', 6.0, 12, '2000-01-06'),
    (8, 'S eight', 8.0, 16, '2000-01-08'),
    (8, 'S eight', 8.0, 16, '2000-01-08'),
    (8, 'S eight', 8.0, 16, '2000-01-08'),
    (8, 'S eight', 8.0, 16, '2000-01-08');

SELECT FROM R, S;

SHOW TABLES;
COMMIT;

SET DEBUG ON;
SET PLANNER NAIVE; -- or BASELINE
ANALYZE;
SET AUTOCOMMIT ON;

SELECT * FROM R, S WHERE A = S.C; -- should be index nested loop on secondary index S(C)
SELECT R.*, S.* FROM S, R WHERE A = S.C; -- should be index nested loop on primary index R(A)
SELECT * FROM R, S WHERE A = F; -- should be sort merge join; baselins is smart enough to avoid sort on R
SELECT R.*, S.* FROM S, R WHERE A = F; -- would like to do sort merge join; but index scan on R(A) takes precedence
SELECT * FROM R, S WHERE R.C = E; -- should be merge sort... cast seems to work
SELECT * FROM R, S WHERE R.C = F; -- should be merge sort
SELECT *, -A FROM R WHERE 3 < A AND A <= 8; -- should be a scan on primary index
SELECT *, A+2 FROM R WHERE A * 2 = C AND A > 10; -- should be index scan followed by filter
SELECT * FROM R WHERE a = TRUE; -- a bit weird, but true can be cast to 1
SELECT * FROM S s1, S s2 WHERE s1.G < s2.G; --- should just be a true block-based nested-loop join, with a cond
-- now try hash joins:
SET SORT_MERGE_JOIN OFF;
SET INDEX_JOIN OFF;
SELECT * FROM R, S WHERE A = F;
SELECT R.*, S.* FROM S, R WHERE A = F;
SELECT * FROM R, S WHERE R.C = E; -- try cast
SELECT * FROM R, S WHERE R.C = F;

-- now try aggr
SELECT A FROM R GROUP BY A HAVING SUM(C) > 5;
SELECT S.C, SUM(S.E) FROM R, S WHERE R.A < S.C GROUP BY S.C HAVING SUM(S.E) > 20;
SELECT C, SUM(E) FROM S group by C HAVING MIN(DISTINCT(G)) > '2000-01-02';
SELECT C, SUM(E) FROM S group by C HAVING MIN(G) > '2000-01-02';
SELECT A, COUNT(*) FROM R GROUP BY A HAVING MIN(C) > 5;
SELECT A, COUNT(*) FROM R GROUP BY A HAVING SUM(C) - 1 > 5;
SELECT A FROM R GROUP BY A HAVING SUM(C) > 5;
SELECT C FROM S WHERE G > '2000-01-02';
SELECT C FROM S group by C HAVING MIN(G) > '2000-01-02';
COMMIT;

SET AUTOCOMMIT OFF;
INSERT INTO S (SELECT C, D, E+100, F, G FROM S);
INSERT INTO S (SELECT C, D, E+100, F, G FROM S);
INSERT INTO S (SELECT C, D, E+100, F, G FROM S);
INSERT INTO s (SELECT C, D, CAST(E AS INT), F, G FROM S WHERE C = 2); -- test automatic conversion from INT to FLOAT when inserting
ANALYZE S;
COMMIT;
