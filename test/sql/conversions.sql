-- test input function, and conversion from old to new format
SELECT 'flags 0 count 20 compression 10 centroids 8 (1000.000000, 1) (2000.000000, 1) (7000.000000, 2) (26000.000000, 4) (84000.000000, 7) (51000.000000, 3) (19000.000000, 1) (20000.000000, 1)'::tdigest;

-- test input of invalid data

-- negative count
SELECT 'flags 0 count -20 compression 10 centroids 8 (1000.000000, 1) (2000.000000, 1) (7000.000000, 2) (26000.000000, 4) (84000.000000, 7) (51000.000000, 3) (19000.000000, 1) (20000.000000, 1)'::tdigest;

-- mismatching count
SELECT 'flags 0 count 21 compression 10 centroids 8 (1000.000000, 1) (2000.000000, 1) (7000.000000, 2) (26000.000000, 4) (84000.000000, 7) (51000.000000, 3) (19000.000000, 1) (20000.000000, 1)'::tdigest;

-- incorrectly sorted centroids
SELECT 'flags 0 count 20 compression 10 centroids 8 (1000.000000, 1) (2000.000000, 1) (1000.000000, 2) (26000.000000, 4) (84000.000000, 7) (51000.000000, 3) (19000.000000, 1) (20000.000000, 1)'::tdigest;

