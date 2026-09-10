Changes for v1.4.6:
- Disable FP contraction to make tests pass on arm64
- Fix builds on 32-bit systems by defining strtoi64
- Fix validation of NULL / empty in several functions
- Clamp results to valid range to handle rounding errors
- Make possibly-long loops interruptible
- Make centroid sort more resilient to invalid data
