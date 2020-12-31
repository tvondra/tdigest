drop table if exists t;
create table t (v double precision);

drop table if exists datasets;
create table datasets (ds_name text, ds_sql text);

insert into datasets values ('uniform', 'with d as (select pow(random(), 1) as v from generate_series(1,%s))
insert into t select v from (select v, generate_series(1, %s + (%s * random())::int) from d) foo');

insert into datasets values ('pow(2)', 'with d as (select pow(random(), 2) as v from generate_series(1,%s))
insert into t select v from (select v, generate_series(1, %s + (%s * random())::int) from d) foo');

insert into datasets values ('pow(4)', 'with d as (select pow(random(), 4) as v from generate_series(1,%s))
insert into t select v from (select v, generate_series(1, %s + (%s * random())::int) from d) foo');

insert into datasets values ('pow(0.5)', 'with d as (select pow(random(), 0.5) as v from generate_series(1,%s))
insert into t select v from (select v, generate_series(1, %s + (%s * random())::int) from d) foo');

insert into datasets values ('pow(0.25)', 'with d as (select pow(random(), 0.25) as v from generate_series(1,%s))
insert into t select v from (select v, generate_series(1, %s + (%s * random())::int) from d) foo');

insert into datasets values ('1 - pow(2)', 'with d as (select 1.0 - pow(random(), 2) as v from generate_series(1,%s))
insert into t select v from (select v, generate_series(1, %s + (%s * random())::int) from d) foo');

insert into datasets values ('1 - pow(4)', 'with d as (select 1.0 - pow(random(), 4) as v from generate_series(1,%s))
insert into t select v from (select v, generate_series(1, %s + (%s * random())::int) from d) foo');

insert into datasets values ('1 - pow(0.5)', 'with d as (select 1.0 - pow(random(), 0.5) as v from generate_series(1,%s))
insert into t select v from (select v, generate_series(1, %s + (%s * random())::int) from d) foo');

insert into datasets values ('1 - pow(0.25)', 'with d as (select 1.0 - pow(random(), 0.25) as v from generate_series(1,%s))
insert into t select v from (select v, generate_series(1, %s + (%s * random())::int) from d) foo');


create or replace function test_queries(npercentiles int, p double precision, nvalues int, minvalues int, maxvalues int, out dataset text,
                                        out simple_random double precision, out simple_asc double precision, out simple_desc double precision,
                                        out preagg_random double precision, out preagg_asc double precision, out preagg_desc double precision,
                                        out simple_asc_cmp double precision, out simple_desc_cmp double precision,
                                        out preagg_random_cmp double precision, out preagg_asc_cmp double precision,
                                        out preagg_desc_cmp double precision)
returns setof record language plpgsql as $$
declare
    d record;
    perc_cont_percs double precision[];
    simple_random_percs double precision[];
    simple_asc_percs double precision[];
    simple_desc_percs double precision[];
    preagg_random_percs double precision[];
    preagg_asc_percs double precision[];
    preagg_desc_percs double precision[];
    percs double precision[];
    run int;
    tmp_simple_random double precision;
    tmp_simple_asc double precision;
    tmp_simple_desc double precision;
    tmp_preagg_random double precision;
    tmp_preagg_asc double precision;
    tmp_preagg_desc double precision;
begin

    raise notice 'percentiles % range % values % min % max %', npercentiles, p, nvalues, minvalues, maxvalues;

    -- generate percentiles
    select array_agg(x) into percs from (
      select i::double precision / npercentiles as x from generate_series(1,npercentiles) s(i)
    ) foo where x <= p or x > 1.0 - p;

    for d in (select * from datasets order by ds_name) loop

        simple_random := 0;
        simple_asc := 0;
        simple_desc := 0;
        preagg_random := 0;
        preagg_asc := 0;
        preagg_desc := 0;

        for run in 1..10 loop

            -- rebuild the table
            execute 'truncate t';
            execute format(d.ds_sql, nvalues, minvalues, (maxvalues - minvalues));
            execute 'analyze t';

            dataset := d.ds_name;

            select percentile_cont(percs) within group (order by v) into perc_cont_percs from (select * from t) d;

            select tdigest_percentile(v, 100, percs) into simple_random_percs from (select * from t order by random()) d;
            select tdigest_percentile(v, 100, percs) into simple_asc_percs from (select * from t order by v) d;
            select tdigest_percentile(v, 100, percs) into simple_desc_percs from (select * from t order by v desc) d;

            select tdigest_percentile(v, c, 100, percs) into preagg_random_percs from (select v, count(*) as c from t group by v order by random()) d;
            select tdigest_percentile(v, c, 100, percs) into preagg_asc_percs from (select v, count(*) as c from t group by v order by v) d;
            select tdigest_percentile(v, c, 100, percs) into preagg_desc_percs from (select v, count(*) as c from t group by v order by v desc) d;

            select sqrt(sum(pow(a-b,2))) into tmp_simple_random from (select unnest(perc_cont_percs) as a, unnest(simple_random_percs) as b) d;
            select sqrt(sum(pow(a-b,2))) into tmp_simple_asc from (select unnest(perc_cont_percs) as a, unnest(simple_asc_percs) as b) d;
            select sqrt(sum(pow(a-b,2))) into tmp_simple_desc from (select unnest(perc_cont_percs) as a, unnest(simple_desc_percs) as b) d;
            select sqrt(sum(pow(a-b,2))) into tmp_preagg_random from (select unnest(perc_cont_percs) as a, unnest(preagg_random_percs) as b) d;
            select sqrt(sum(pow(a-b,2))) into tmp_preagg_asc from (select unnest(perc_cont_percs) as a, unnest(preagg_asc_percs) as b) d;
            select sqrt(sum(pow(a-b,2))) into tmp_preagg_desc from (select unnest(perc_cont_percs) as a, unnest(preagg_desc_percs) as b) d;

            simple_random := simple_random + tmp_simple_random / 10;
            simple_asc := simple_asc + tmp_simple_asc / 10;
            simple_desc := simple_desc + tmp_simple_desc / 10;
            preagg_random := preagg_random + tmp_preagg_random / 10;
            preagg_asc := preagg_asc + tmp_preagg_asc / 10;
            preagg_desc := preagg_desc + tmp_preagg_desc / 10;

        end loop;

        simple_asc_cmp := round((simple_asc / simple_random)::numeric, 2);
        simple_desc_cmp := round((simple_desc / simple_random)::numeric, 2);
        preagg_random_cmp := round((preagg_random / simple_random)::numeric, 2);
        preagg_asc_cmp := round((preagg_asc / simple_random)::numeric, 2);
        preagg_desc_cmp := round((preagg_desc / simple_random)::numeric, 2);

        simple_random := round(simple_random::numeric, 6);
        simple_asc := round(simple_asc::numeric, 6);
        simple_desc := round(simple_desc::numeric, 6);
        preagg_random := round(preagg_random::numeric, 6);
        preagg_asc := round(preagg_asc::numeric, 6);
        preagg_desc := round(preagg_desc::numeric, 6);

        return next;

    end loop;

    return;

end;
$$;

select * from test_queries(1000, 0.01, 10000, 1, 1);
select * from test_queries(1000, 0.05, 10000, 1, 1);
select * from test_queries(1000, 0.1, 10000, 1, 1);
select * from test_queries(1000, 0.2, 10000, 1, 1);
select * from test_queries(1000, 0.3, 10000, 1, 1);
select * from test_queries(1000, 0.4, 10000, 1, 1);
select * from test_queries(1000, 0.5, 10000, 1, 1);

select * from test_queries(1000, 0.01, 1000, 10, 20);
select * from test_queries(1000, 0.05, 1000, 10, 20);
select * from test_queries(1000, 0.1, 1000, 10, 20);
select * from test_queries(1000, 0.2, 1000, 10, 20);
select * from test_queries(1000, 0.3, 1000, 10, 20);
select * from test_queries(1000, 0.4, 1000, 10, 20);
select * from test_queries(1000, 0.5, 1000, 10, 20);

select * from test_queries(1000, 0.01, 10000, 10, 20);
select * from test_queries(1000, 0.05, 10000, 10, 20);
select * from test_queries(1000, 0.1, 10000, 10, 20);
select * from test_queries(1000, 0.2, 10000, 10, 20);
select * from test_queries(1000, 0.3, 10000, 10, 20);
select * from test_queries(1000, 0.4, 10000, 10, 20);
select * from test_queries(1000, 0.5, 10000, 10, 20);
