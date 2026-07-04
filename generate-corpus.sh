#!/usr/bin/env bash

mkdir -p corpus/in corpus/recv

for id in $(seq 1 100); do

	compress=$((10 + RANDOM % 9990))
	rows=$((1 + $RANDOM))

<<<<<<< HEAD
	psql -qAt -c "select encode(tdigest_send((select tdigest(random(), $compress) from generate_series(1, $rows))),'base64')" test | base64 -d > corpus/recv/$id
	psql -qAt -c "select tdigest(random(), $compress) from generate_series(1, $rows)" test > corpus/in/$id
||||||| parent of 48004e4 (harness for fuzzing input functions)
	psql -qAt -c "select encode(tdigest_send((select tdigest(random(), $compress) from generate_series(1, $rows))),'base64')" test | base64 -d > corpus/$id
=======
	psql -qAt -z -0 -c "select tdigest(random(), $compress) from generate_series(1, $rows)" test > corpus/in/$id
	psql -qAt -c "select encode(tdigest_send((select tdigest(random(), $compress) from generate_series(1, $rows))),'base64')" test | base64 -d > corpus/recv/$id
>>>>>>> 48004e4 (harness for fuzzing input functions)

done
