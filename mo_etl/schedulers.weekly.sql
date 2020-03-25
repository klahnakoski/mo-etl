SELECT `week`,
       num_pushes,
       num_seta / num_pushes           seta_scheduled,
       num_relevant_tests / num_pushes relevant_tests_scheduled,
       num_bugbug / num_pushes         bugbug_scheduled,
       num_bad,
    -- proportion where indicator was scheduled on bad push
    num_seta_hit/num_bad as seta_hit_rate,
    num_relevant_hit/num_bad as relevant_tests_hit_rate,
    num_bugbug_hit/num_bad bugbug_hit_rate,
    # hits per 1000 tasks scheduled
    1000*num_seta_hit*num_pushes/num_bad/num_seta as seta_eff,
    1000*num_relevant_hit*num_pushes/num_bad/num_relevant_tests as relevant_tests_eff,
    1000*num_bugbug_hit*num_pushes/num_bad/num_bugbug as bugbug_eff,
FROM (
    SELECT
    DATE_TRUNC(DATE (push_date, "GMT"), ISOWEEK) AS `week`,
    count(push_id) AS num_pushes,
    count(CASE WHEN indicators>0 THEN 1 END) AS num_bad,
    -- TOTAL SCHEDULED TASKS
    sum(seta) as num_seta,
    sum(relevant_tests) as num_relevant_tests,
    sum(bugbug) as num_bugbug,
    -- COUNT WHEN SCHEDULED ON BAD PUSH
    count(CASE WHEN seta_count>0 THEN 1 END) AS num_seta_hit,
    count(CASE WHEN relevant_tests_count>0 THEN 1 END) num_relevant_hit,
    count(CASE WHEN bugbug_count>0 THEN 1 END) num_bugbug_hit,
    FROM
    `moz-fx-dev-ekyle-treeherder`.dev_2d_scheduling.view_schedulers_overview
    GROUP BY
    `week`
    ) a
order by
    `week`
    limit
    1000