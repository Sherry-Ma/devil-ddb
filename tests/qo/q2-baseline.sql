-- baseline plan: 12396 (Estimated IO): MergeJoin( MergeJoin (T10K_1, T10K_2), T100)
-- example plan: 6430 (Estimated IO): BNLJoin ( BNLJoin (T100, T10K_1), T10K_2))
-- Result = 32


SET AUTOCOMMIT OFF;
set planner baseline;
-- set debug on;
analyze;



select count(*) from T10K as T10K_1, T10K as T10K_2, T100
where T100.A = T10K_1.A
AND T10K_1.B = T10K_2.A
AND T10K_2.B >= 125
AND T100.C > 0.001
;

ROLLBACK;