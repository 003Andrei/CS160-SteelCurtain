#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <stdexcept>
#include <algorithm>
#include <chrono>
#include <functional>
#include <thread>

// ---------------------------------------------------------
// 1. Data Structures
// ---------------------------------------------------------
struct CSRGraph {
    int num_vertices;
    std::vector<int> offsets;
    std::vector<int> edges;
};

using QueryCallback = std::function<std::string(const CSRGraph&, int src, int K)>;

struct QueryTask {
    int src;
    int K;
    QueryCallback cb;
    std::string result;
    std::string expectedResult; // For correctness verification
};

// ---------------------------------------------------------
// 2. Graph Loading (from earlier)
// ---------------------------------------------------------
CSRGraph LoadGraph(const char *filename) {
    std::ifstream fin(filename);
    if (!fin) throw std::runtime_error(std::string("Failed to open file: ") + filename);

    std::string line;
    int declared_nodes = -1;
    std::vector<std::pair<int, int>> edge_list;
    edge_list.reserve(1000000);

    int max_vertex = -1;

    while (std::getline(fin, line)) {
        if (line.empty()) continue;
        if (line[0] == '#') {
            std::size_t pos = line.find("Nodes:");
            if (pos != std::string::npos) {
                std::stringstream ss(line.substr(pos + 6));
                ss >> declared_nodes;
            }
            continue;
        }

        int u, v;
        if (sscanf(line.c_str(), "%d %d", &u, &v) == 2) {
            edge_list.push_back({u, v});
            max_vertex = std::max(max_vertex, std::max(u, v));
        }
    }

    int num_vertices = (declared_nodes != -1) ? declared_nodes : (max_vertex + 1);

    CSRGraph g;
    g.num_vertices = num_vertices;
    g.offsets.assign(num_vertices + 1, 0);

    for (const auto &e : edge_list) {
        g.offsets[e.first + 1]++;
    }
    for (int i = 0; i < num_vertices; ++i) {
        g.offsets[i + 1] += g.offsets[i];
    }

    g.edges.assign(edge_list.size(), -1);
    std::vector<int> next_pos = g.offsets;
    for (const auto &e : edge_list) {
        g.edges[next_pos[e.first]++] = e.second;
    }

    return g;
}

// ---------------------------------------------------------
// 3. BFS K-Hop Logic & Callbacks
// ---------------------------------------------------------

// Helper function to find all reachable nodes within K hops
std::vector<int> get_k_hop_reachable(const CSRGraph& g, int src, int K) {
    std::vector<int> reachable;
    if (src < 0 || src >= g.num_vertices || K == 0) return reachable;

    std::vector<bool> visited(g.num_vertices, false);
    std::vector<int> current_level;

    current_level.push_back(src);
    visited[src] = true; // Exclude source from being counted as a reached neighbor

    for (int step = 0; step < K; ++step) {
        std::vector<int> next_level;
        for (int u : current_level) {
            int start = g.offsets[u];
            int end = g.offsets[u + 1];
            for (int i = start; i < end; ++i) {
                int v = g.edges[i];
                if (!visited[v]) {
                    visited[v] = true;
                    next_level.push_back(v);
                    reachable.push_back(v);
                }
            }
        }
        if (next_level.empty()) break; // Early exit if no more reachable nodes
        current_level = std::move(next_level);
    }
    return reachable;
}

std::string count_cb(const CSRGraph& g, int src, int K) {
    auto reachable = get_k_hop_reachable(g, src, K);
    return std::to_string(reachable.size());
}

std::string max_cb(const CSRGraph& g, int src, int K) {
    auto reachable = get_k_hop_reachable(g, src, K);
    if (reachable.empty()) return "-1"; // Return -1 if no reachable nodes

    int max_id = -1;
    for (int v : reachable) {
        if (v > max_id) max_id = v;
    }
    return std::to_string(max_id);
}

// ---------------------------------------------------------
// 4. Task Runners
// ---------------------------------------------------------
void RunTasksSequential(const CSRGraph& g, std::vector<QueryTask>& tasks) {
    for (auto& task : tasks) {
        task.result = task.cb(g, task.src, task.K);
    }
}

void RunTasksParallel(const CSRGraph& g, std::vector<QueryTask>& tasks, int num_threads) {
    std::vector<std::thread> threads;
    int n = tasks.size();

    // Calculate how many tasks each thread should process (chunking)
    int chunk_size = (n + num_threads - 1) / num_threads;

    for (int t = 0; t < num_threads; ++t) {
        int start = t * chunk_size;
        int end = std::min(start + chunk_size, n);

        if (start >= end) break; // No more work left

        threads.emplace_back([&g, &tasks, start, end]() {
            for (int i = start; i < end; ++i) {
                tasks[i].result = tasks[i].cb(g, tasks[i].src, tasks[i].K);
            }
        });
    }

    // Wait for all threads to finish
    for (auto& t : threads) {
        t.join();
    }
}

// ---------------------------------------------------------
// 5. Utility: Load Queries & Verify
// ---------------------------------------------------------
std::vector<QueryTask> LoadQueries(const char* filename) {
    std::vector<QueryTask> tasks;
    std::ifstream fin(filename);
    if (!fin) throw std::runtime_error(std::string("Failed to open query file: ") + filename);

    std::string line;
    while (std::getline(fin, line)) {
        if (line.empty() || line[0] == '#') continue;

        int src, K, type;
        std::string expected;
        std::stringstream ss(line);
        if (ss >> src >> K >> type >> expected) {
            QueryTask task;
            task.src = src;
            task.K = K;
            task.expectedResult = expected;
            task.cb = (type == 1) ? count_cb : max_cb;
            tasks.push_back(task);
        }
    }
    return tasks;
}

int verify_correctness(const std::vector<QueryTask>& tasks) {
    int errors = 0;
    for (size_t i = 0; i < tasks.size(); ++i) {
        if (tasks[i].result != tasks[i].expectedResult) {
            std::cerr << "Mismatch on task " << i << "! Expected: "
                      << tasks[i].expectedResult << ", Got: " << tasks[i].result << "\n";
            errors++;
        }
    }
    return errors;
}

// ---------------------------------------------------------
// 6. Main Orchestrator
// ---------------------------------------------------------
int main() {
    std::cout << "--- Loading Graph ---\n";
    CSRGraph graph = LoadGraph("/home/captain/CLionProjects/CS160/UCR-CS160-Project/Phase-1/soc-Slashdot0902.txt");
    std::cout << "Nodes: " << graph.num_vertices << " | Edges: " << graph.edges.size() << "\n\n";

    // You can switch this to "queries10000.txt" for performance evaluation
    std::string query_file = "/home/captain/CLionProjects/CS160/UCR-CS160-Project/Phase-1/queries20.txt";
    std::cout << "--- Loading Queries (" << query_file << ") ---\n";
    auto tasks = LoadQueries(query_file.c_str());
    std::cout << "Loaded " << tasks.size() << " queries.\n\n";

    // Sequential Run
    std::vector<QueryTask> seq_tasks = tasks;
    auto t1 = std::chrono::high_resolution_clock::now();
    RunTasksSequential(graph, seq_tasks);
    auto t2 = std::chrono::high_resolution_clock::now();
    double seq_time = std::chrono::duration<double>(t2 - t1).count();

    std::cout << "--- Sequential Execution ---\n";
    std::cout << "Time: " << seq_time << " seconds\n";
    int seq_errors = verify_correctness(seq_tasks);
    std::cout << "Errors: " << seq_errors << "\n\n";

    // Parallel Run
    std::vector<QueryTask> par_tasks = tasks;
    int num_threads = std::thread::hardware_concurrency(); // Use available CPU cores
    if (num_threads == 0) num_threads = 4; // Fallback

    auto t3 = std::chrono::high_resolution_clock::now();
    RunTasksParallel(graph, par_tasks, num_threads);
    auto t4 = std::chrono::high_resolution_clock::now();
    double par_time = std::chrono::duration<double>(t4 - t3).count();

    std::cout << "--- Concurrent Execution (" << num_threads << " threads) ---\n";
    std::cout << "Time: " << par_time << " seconds\n";
    int par_errors = verify_correctness(par_tasks);
    std::cout << "Errors: " << par_errors << "\n\n";

    // Speedup Output
    std::cout << "--- Performance Comparison ---\n";
    std::cout << "Speedup: " << seq_time / par_time << "x\n";

    return 0;
}