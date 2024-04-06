-- baseline plan: 43795 (Estimated IO): MergeJoin(T101k, T100)
-- example plan: 1819 (Estimated IO): IndexNLJoin (T100, T101K)
-- naive: 43457
-- baseline: 84

SET AUTOCOMMIT OFF;
set planner baseline;
-- set debug on;
-- analyze;


CREATE INDEX ON T101K(B);

select * from T101K
where T101K.B + 1 = 89
;

ROLLBACK;