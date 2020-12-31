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

create or replace function query_timing(query text, loops int = 10, out avg_time double precision, out stdev_time double precision) returns record
language plpgsql as
$$
declare
    timings double precision[] := NULL;
    i int;
    start_ts timestamptz;
    end_ts timestamptz;
    delta_ts double precision;
    total_ts double precision;
    r record;
begin

    total_ts := 0;

    for i in 1..loops loop

        start_ts := clock_timestamp();
        execute $1;
        end_ts := clock_timestamp();

        delta_ts := 1000 * (extract(epoch from end_ts) - extract(epoch from start_ts));

        timings := array_append(timings, delta_ts);
        total_ts := total_ts + delta_ts;

    end loop;

    avg_time := (total_ts / loops);
    stdev_time := 0.0;

    for r in select unnest(timings) as t loop
        stdev_time := stdev_time + pow(r.t - avg_time,2);
    end loop;

    stdev_time := sqrt(stdev_time / loops);

    avg_time := round(avg_time::numeric, 3);

    return;

end;
$$;

create or replace function test_queries(nvalues int, minvalues int, maxvalues int, out dataset text,
                                        out simple_random double precision, out simple_asc double precision, out simple_desc double precision,
                                        out preagg_random double precision, out preagg_asc double precision, out preagg_desc double precision)
returns setof record language plpgsql as $$
declare
    d record;
begin

    raise notice 'values % min % max %', nvalues, minvalues, maxvalues;

    for d in (select * from datasets order by ds_name) loop

        -- rebuild the table
        execute 'truncate t';
        execute format(d.ds_sql, nvalues, minvalues, (maxvalues - minvalues));
        execute 'analyze t';

        dataset := d.ds_name;

        select q.avg_time into simple_random from query_timing('select tdigest(v, 100) from (select * from t order by random()) d') q;
        select q.avg_time into simple_asc from query_timing('select tdigest(v, 100) from (select * from t order by v) d') q;
        select q.avg_time into simple_desc from query_timing('select tdigest(v, 100) from (select * from t order by v desc) d') q;

        select q.avg_time into preagg_random from query_timing('select tdigest(v, c, 100) from (select v, count(*) as c from t group by v order by random()) d') q;
        select q.avg_time into preagg_asc from query_timing('select tdigest(v, c, 100) from (select v, count(*) as c from t group by v order by v) d') q;
        select q.avg_time into preagg_desc from query_timing('select tdigest(v, c, 100) from (select v, count(*) as c from t group by v order by v desc) d') q;

        return next;

    end loop;

    return;

end;
$$;

select * from test_queries(1000, 1, 1);
select * from test_queries(10000, 1, 1);
select * from test_queries(100000, 1, 1);

select * from test_queries(1000, 5, 10);
select * from test_queries(10000, 5, 10);
select * from test_queries(100000, 5, 10);

select * from test_queries(1000, 20, 40);
select * from test_queries(10000, 20, 40);
select * from test_queries(100000, 20, 40);
