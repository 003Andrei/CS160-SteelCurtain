#include <iostream>
#include <vector>
#include <fstream>
#include <string>
#include <cstdio>
#include <functional>
#include <utility>
using namespace std;

struct CSRGraph {
    int num_vertices;
    vector<int> offsets;
    vector<int> edges;
};
CSRGraph LoadGraph(const char *filename) {
    ifstream file(filename);
    if (!file.is_open()) {
        cerr << "Failed to open " << filename << "\n";
        return {0, {}, {}};
    }
    string line;
    vector<pair<int, int>> edge_list;
    int max_vertex = -1;

    while (getline(file, line)) {
        if (line.empty() || line[0] == '#') continue;
        int u, v;
        if (sscanf(line.c_str(), "%d %d", &u, &v) == 2) {
            edge_list.push_back({u, v});
            if (u > max_vertex) max_vertex = u;
            if (v > max_vertex) max_vertex = v;
        }
    }

    CSRGraph graph;
    graph.num_vertices = max_vertex + 1;
    graph.edges.resize(edge_list.size());
    graph.offsets.assign(graph.num_vertices + 1, 0);

    for (size_t i = 0; i < edge_list.size(); ++i) {
        graph.offsets[edge_list[i].first + 1]++;
    }
    for (int i = 0; i < graph.num_vertices; ++i) {
        graph.offsets[i + 1] += graph.offsets[i];
    }

    vector<int> current_offset = graph.offsets;
    for (size_t i = 0; i < edge_list.size(); ++i) {
        int u = edge_list[i].first;
        int v = edge_list[i].second;
        graph.edges[current_offset[u]++] = v;
    }

    return graph;
}

//
// Query Task
//
using QueryCallback = function<string(const CSRGraph&, int src, int K)>;

struct QueryTask {
    int src;
    int K;
    QueryCallback cb;
    string result;
};

void RunTasksSequential(const CSRGraph& g, vector<QueryTask>& tasks);

// execute tasks concurrently with num_threads threads
void RunTasksParallel(const CSRGraph& g, vector<QueryTask>& tasks, int num_threads);


int main() {
    cout << "Loading graph...\n";
    CSRGraph graph = LoadGraph("/home/captain/CLionProjects/CS160/soc-Slashdot0902.txt");
    if (graph.num_vertices == 0) {
        return 1;
    }

    cout << "Graph is completed\n\n";
    cout << "Number of vertices: " << graph.num_vertices << "\n";
    cout << "Number of edges: " << graph.edges.size() << "\n\n";

    int test_six = 6;

    if (test_six < graph.num_vertices) {
        int start = graph.offsets[test_six];
        int end = graph.offsets[test_six + 1];
        cout << "Vertex " << test_six << " Neighbors: ";
        if (start == end) {
            cout << "None";
        }
        else {
            for (int i = start; i < end; ++i) {
                cout << graph.edges[i] << " ";
            }
        }
        cout << "\n";
    }
    else {
        cout << "Vertex out of bounds.\n";
    }



    return 0;
}