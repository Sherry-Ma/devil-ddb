-- baseline plan: 48961 (Estimated IO): IndexNLJoin ( IndexNLJoin ( MergeJoin ( IndexNLJoin (MergeJoin (T10K, T100_1) T1k_1), T100_2), T1k_2), T100K)
-- example plan: 15170 (Estimated IO): MergeJoin ( MergeJoin ( MergeJoin( T10K, MergeJoin ( IndexNLJoin ( T1k_1, T100K), T100_1)) T1k_2), T100_2)
-- Result = 679


SET AUTOCOMMIT OFF;
set planner baseline;
-- set debug on;
analyze;

CREATE INDEX ON T100K(B);

select count(*) from T10K, T100 as T100_1, T1k as T1k_1, T100 as T100_2, T1k as T1k_2, T100K
where T10K.B = T100_1.A
AND T1K_1.A = T100K.A
AND T100_1.B = T100_2.B
AND T10K.A = T1k_2.A
AND T100_1.A = T1k_1.A
AND T100K.B > 11
;

ROLLBACK;