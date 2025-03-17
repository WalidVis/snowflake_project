create or replace stream BRONZE_LAYER.PRC_BENCHMARK_BRZ_STREAM 
on table PRC_BENCHMARK_BRZ append_only = true;
