-- baseline plan: 6974 (Estimated IO): MergeJoin ( IndexNLJoin ( MergeJoin (T10K, T100_1), T1k), T100_2)
-- example plan: 3250 (Estimated IO): BNLJoin ( BNLJoin ( BNLJoin (T1k, T100_1), T10K)), T100_2)
-- Result = 45


SET AUTOCOMMIT OFF;
set planner baseline;
-- set debug on;
analyze;

CREATE INDEX ON T100K(B);

select count(*) from T10K, T100 as T100_1, T1k, T100 as T100_2
where T10K.B = T100_1.A
AND T100_1.B = T100_2.B
AND T100_1.A = T1k.A
AND T10K.A > 10
AND T1k.A = 99
;

ROLLBACK;