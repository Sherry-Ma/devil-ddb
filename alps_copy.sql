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
    (8, 'S eight', 8.0, 16, '2000-01-08');
SHOW TABLES;
COMMIT;

SET DEBUG OFF;


-- now try hash joins:
SET SORT_MERGE_JOIN OFF;
SET INDEX_JOIN OFF;

SELECT * FROM R, S WHERE A = F;


COMMIT;
