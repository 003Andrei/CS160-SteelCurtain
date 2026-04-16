# Phase 1: Concurrent Local Queries

## Introduction

In this phase, you will load a directed graph from an edge list file into memory using the Compressed Sparse Row (CSR) format, and implement K-hop neighborhood queries. Each query explores vertices reachable within K hops from a given source vertex (following outgoing edges) and computes a result using a user-defined callback.

You will then evaluate a sequence of such queries, first sequentially and then concurrently using multiple threads, and compare their execution time.

## Requirements

1. **Graph Loading**: Read an input graph in edge list format and store it in memory using the CSR representation.

2. **Query Callback**: Provide a callback interface that allows developers to implement custom logic for processing the K-hop query results. You should support at least the following queries:
   - Count the number of reachable nodes within K hops (excluding the source vertex itself).
   - Find the maximum node ID among the reachable nodes within K hops (excluding the source vertex itself).

3. **Sequential Evaluation**: Execute a given sequence of K-hop queries one by one and record the total execution time.

4. **Concurrent Evaluation**: Execute the same sequence of K-hop queries concurrently using multiple threads and record the total execution time.

5. **Performance Comparison**: Measure and compare the execution time between sequential and concurrent evaluation. Report your findings with analysis in the development journal.

## Hints

### Edge List Format

The input graph is stored as a text file where each line represents a directed edge:

```
src dst
```

Lines starting with `#` are comments and should be skipped. Node IDs are integers. Example:

```
# example graph
# FromNodeId ToNodeId
0 1
0 2
1 3
2 3
```

### Query File Format

A query file is a text file where each line specifies a query:

```
src K queryType expectedResult
```

- `queryType`: 1 = count reachable nodes, 2 = max node ID (returns -1 if no reachable nodes)
- `expectedResult`: the expected output for correctness verification

Example:

```
# src K queryType(1=count,2=max) expectedResult
15795 1 1 20
76820 3 2 53604
37194 3 2 -1
```

We provide two query files: `queries20.txt` (20 queries, for debugging) and `queries10000.txt` (10,000 queries, for performance evaluation).

### Data Structures & Interfaces

The following definitions are provided as a starting point. Feel free to adapt or redesign them to better suit your implementation.

```cpp
struct CSRGraph {
    int num_vertices;
    std::vector<int> offsets;  // size = num_vertices + 1
    std::vector<int> edges;    // concatenated adjacency lists
    // offsets[i]..offsets[i+1] gives the range of neighbors for vertex i in edges[]
};

using QueryCallback = std::function<std::string(const CSRGraph&, int src, int K)>;

struct QueryTask {
    int src;
    int K;
    QueryCallback cb;
    std::string result;
};

void RunTasksSequential(const CSRGraph& g, std::vector<QueryTask>& tasks);

// execute tasks concurrently with num_threads threads
void RunTasksParallel(const CSRGraph& g, std::vector<QueryTask>& tasks, int num_threads);
```

### Pseudocode

The following pseudocode illustrates the overall workflow. It is provided as a reference, and you may organize your code differently.

```
// 1. Load graph
graph = LoadGraph("edges.txt")

// 2. Define callbacks
count_cb = (graph, src, K) -> return count of K-hop reachable nodes
max_cb   = (graph, src, K) -> return max node ID among K-hop reachable nodes

// 3. Load query file and build tasks
for each line (src, K, queryType, expectedResult) in "queries.txt":
    cb = (queryType == 1) ? count_cb : max_cb
    tasks.append(QueryTask(src, K, cb))

// 4. Run sequentially, record time
seq_tasks = copy(tasks)
t1 = now()
RunTasksSequential(graph, seq_tasks)
sequential_time = now() - t1

// 5. Run concurrently, record time
par_tasks = copy(tasks) // copy avoid overwriting results
t2 = now()
RunTasksParallel(graph, par_tasks, num_threads=4)
parallel_time = now() - t2

// 6. Compare and report
print(sequential_time, parallel_time)
```

## Testing & Verification

To verify the correctness of your implementation, we recommend the following approach:

1. **Construct your own test cases**: Start with small graphs where you can manually compute the expected K-hop results. For example:
   - A graph with cycles to ensure visited nodes are not revisited
   - A star graph to test fan-out behavior
   - Edge cases: isolated vertices, K=0, source vertex not in graph

2. **Compare sequential and concurrent results**: Run the same set of queries using both `RunTasksSequential` and `RunTasksParallel`, and verify that they produce identical results.

3. **Verify on the provided dataset**: Once small cases pass, run your implementations on the provided dataset ([download link](https://drive.google.com/drive/folders/1Cr4QkLBpWa3Gp0u-9YWNE9voH9Dz4INg)). The dataset includes `soc-Slashdot0902.txt` (the edge list file) and query files. Use `queries20.txt` to debug correctness by comparing your output against the expected results. Then use `queries10000.txt` for performance evaluation. You should expect a noticeable speedup from the concurrent version. Report the correctness and performance results on this dataset in your development journal.
