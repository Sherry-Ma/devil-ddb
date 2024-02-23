SET AUTOCOMMIT OFF;
SET INDEX_JOIN OFF;
SET SORT_MERGE_JOIN OFF;
CREATE TABLE R(A FLOAT, B INT, C FLOAT);
INSERT INTO R VALUES
	(0.38, 4, 0.49),
	(0.2, 13, 0.72),
	(0.81, 16, 0.59),
	(0.2, 19, 0.93),
	(0.38, 14, 0.27),
	(0.21, 11, 0.35),
	(0.81, 2, 0.32),
	(0.5, 6, 0.74),
	(0.81, 0, 0.88),
	(0.38, 18, 0.39),
	(0.2, 8, 0.92),
	(0.38, 12, 0.77),
	(0.2, 18, 0.1),
	(0.81, 14, 0.97),
	(0.81, 0, 0.65),
	(0.21, 15, 0.53),
	(0.38, 9, 0.38),
	(0.48, 18, 0.97),
	(0.21, 14, 0.76),
	(0.21, 15, 0.94);
CREATE TABLE S(D FLOAT, E INT, F FLOAT, G VARCHAR);
INSERT INTO S VALUES
	(0.87, 6, 0.73, 'Q'),
	(0.21, 11, 0.09, 'X'),
	(0.21, 6, 0.88, '1'),
	(0.81, 15, 0.5, '7'),
	(0.2, 14, 0.66, 'V'),
	(0.2, 3, 0.01, 'H'),
	(0.87, 9, 0.65, 'G'),
	(0.87, 5, 0.85, 'W'),
	(0.81, 6, 0.85, 'R'),
	(0.21, 11, 0.4, 'X'),
	(0.2, 14, 0.55, 'O'),
	(0.21, 2, 0.16, '4'),
	(0.81, 8, 0.97, 'S'),
	(0.2, 5, 0.85, 'M'),
	(0.81, 14, 0.19, 'K');
ANALYZE;
SELECT * FROM R, S WHERE R.A = S.D;
ROLLBACK;