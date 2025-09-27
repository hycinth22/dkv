#include <benchmark/benchmark.h>

void SomeFunction(double &x) {
    x *= 7.1;
    x /= 442.456;
}

static void BM_SomeFunction(benchmark::State& state) {
  // Perform setup here
  double some_value = 42.0;
  for (auto _ : state) {
    // This code gets timed
    SomeFunction(some_value);
  }
}
// Register the function as a benchmark
BENCHMARK(BM_SomeFunction);
// Run the benchmark
BENCHMARK_MAIN();