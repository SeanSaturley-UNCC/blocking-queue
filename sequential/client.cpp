#include <iostream>
#include <string>
#include <queue>
#include <unordered_set>
#include <cstdio>
#include <cstdlib>
#include <curl/curl.h>
#include <stdexcept>
#include "rapidjson/error/error.h"
#include "rapidjson/reader.h"

struct ParseException : std::runtime_error, rapidjson::ParseResult {
    ParseException(rapidjson::ParseErrorCode code, const char* msg, size_t offset) : 
        std::runtime_error(msg), 
        rapidjson::ParseResult(code, offset) {}
};

#define RAPIDJSON_PARSE_ERROR_NORETURN(code, offset) \
    throw ParseException(code, #code, offset)

#include <rapidjson/document.h>
#include <chrono>

// added these for threading and locking
#include <thread>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <atomic>
#include <algorithm>

using namespace std;
using namespace rapidjson;

bool debug = false;

// Updated service URL
const string SERVICE_URL = "http://hollywood-graph-crawler.bridgesuncc.org/neighbors/";

// Function to HTTP ecnode parts of URLs. for instance, replace spaces with '%20' for URLs
string url_encode(CURL* curl, string input) {
  char* out = curl_easy_escape(curl, input.c_str(), input.size());
  string s = out;
  curl_free(out);
  return s;
}

// Callback function for writing response data
size_t WriteCallback(void* contents, size_t size, size_t nmemb, string* output) {
    size_t totalSize = size * nmemb;
    output->append((char*)contents, totalSize);
    return totalSize;
}

// Function to fetch neighbors using libcurl with debugging
// Note: this function uses the provided CURL* handle (so it can be reused per-thread)
string fetch_neighbors(CURL* curl, const string& node) {

    string url = SERVICE_URL + url_encode(curl, node);
    string response;

    if (debug)
      cout << "Sending request to: " << url << endl;

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    // curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L); // Verbose Logging

    // Set a User-Agent header to avoid potential blocking by the server
    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "User-Agent: C++-Client/1.0");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    // small timeout to avoid infinite blocking on Centaurus
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 10L);

    CURLcode res = curl_easy_perform(curl);

    if (res != CURLE_OK) {
        cerr << "CURL error: " << curl_easy_strerror(res) << endl;
    } else {
      if (debug)
        cout << "CURL request successful!" << endl;
    }

    // Cleanup
    curl_slist_free_all(headers);

    if (debug) 
      cout << "Response received: " << response << endl;  // Debug log

    return (res == CURLE_OK) ? response : "{}";
}

// Function to parse JSON and extract neighbors
vector<string> get_neighbors(const string& json_str) {
    vector<string> neighbors;
    try {
      Document doc;
      doc.Parse(json_str.c_str());
      
      if (doc.HasMember("neighbors") && doc["neighbors"].IsArray()) {
        for (const auto& neighbor : doc["neighbors"].GetArray())
          neighbors.push_back(neighbor.GetString());
      }
    } catch (const ParseException& e) {
      std::cerr<<"Error while parsing JSON: "<<json_str<<std::endl;
      throw e;
    }
    return neighbors;
}

// BFS Traversal Function
vector<string> bfs(CURL* curl, const string& start, int depth) {
    queue<pair<string, int>> q;
    unordered_set<string> visited;
    vector<string> result;

    q.push({start, 0});
    visited.insert(start);

    while (!q.empty()) {
        auto [node, level] = q.front();
        q.pop();

        if (level <= depth) {
            result.push_back(node);
        }
        
        if (level < depth) {
            try {
              for (const auto& neighbor : get_neighbors(fetch_neighbors(curl, node))) {
                if (!visited.count(neighbor)) {
                  visited.insert(neighbor);
                  q.push({neighbor, level + 1});
                }
              }
            } catch (const ParseException& e) {
              std::cerr<<"Error while fetching neighbors of: "<<node<<std::endl;
              throw e;
            }
        }
    }
    return result;
}


// BlockingQueue code
template<typename T>
class BlockingQueue {
public:
    BlockingQueue(): done(false) {}

    void push(T item) {
        {
            std::lock_guard<std::mutex> lk(mtx);
            q.push(std::move(item));
        }
        cv.notify_one(); // used to wake up any waiting/sleeping threads
    }

    // Pop blocks until item available or done flag
    bool pop(T &out) {
        
        std::unique_lock<std::mutex> lk(mtx);
        cv.wait(lk, [&]{ return !q.empty() || done; });
        if (q.empty()) return false;
        
        out = std::move(q.front());
        q.pop();
        return true;
    }



    void set_done() {
        {
            std::lock_guard<std::mutex> lk(mtx);
            done = true;
        }
        cv.notify_all();
    }



    bool empty() {
        std::lock_guard<std::mutex> lk(mtx);
        return q.empty();
        
    }
    void notify_all() { cv.notify_all(); }



private:
    std::queue<T> q;
    std::mutex mtx;

    std::condition_variable cv;

    bool done;
};

// work item for BFS
struct WorkItem {

    std::string node;
    int depth;
};

// Parallel BFS using blocking queue
vector<string> bfs_parallel(const string& start_node, int max_depth, int num_threads) {
    
    BlockingQueue<WorkItem> queue;
    unordered_set<string> visited;
    
    std::mutex visited_mtx;
    std::atomic<int> active_workers{0};


    visited.insert(start_node);
    
    queue.push(WorkItem{start_node, 0});

    // lambda
    auto worker = [&](int tid) {
    
        CURL* thread_curl = curl_easy_init();
        
        if (!thread_curl) {
            cerr << "[ERROR] thread " << tid << " failed to init curl\n";
            return;
        }

        while (true) {
            WorkItem it;

            
            bool ok = queue.pop(it);
            if (!ok) break; // if done and empty

            active_workers.fetch_add(1, std::memory_order_relaxed);

            
            if (it.depth < max_depth) {
                string raw = fetch_neighbors(thread_curl, it.node);
                vector<string> neighs;
                try {
                    neighs = get_neighbors(raw);
                    
                } catch (const ParseException& e) {
                    
                    cerr << "[WARN] parse error for node '" << it.node << "' on thread " << tid << "\n";
                    neighs.clear();
                    
                }

                for (const auto &nb : neighs) {
                    bool push_this = false;
                    {
                        
                        std::lock_guard<std::mutex> lk(visited_mtx);
                        if (visited.find(nb) == visited.end()) {
                            visited.insert(nb);

                            
                            push_this = true;
                        }
                        
                    }
                    if (push_this) {
                        queue.push(WorkItem{nb, it.depth + 1});
                    }
                }
            }

            int after = active_workers.fetch_sub(1, std::memory_order_relaxed) - 1;

            // set to done if therea re no active workers
            if (after == 0 && queue.empty()) {
                queue.set_done();
            }
        } 

        curl_easy_cleanup(thread_curl);
    }; 

    
    vector<thread> threads;
    
    threads.reserve(num_threads);
    for (int i = 0; i < num_threads; ++i) threads.emplace_back(worker, i);
    
    for (auto &t : threads) if (t.joinable()) t.join();
    vector<string> results;
    {
        std::lock_guard<std::mutex> lk(visited_mtx);
        results.assign(visited.begin(), visited.end());
    }
    sort(results.begin(), results.end()); 
    return results;
}

// -------------------- MAIN --------------------
int main(int argc, char* argv[]) {
    if (argc != 3 && argc < 4) {
        cerr << "Usage (backward-compatible): " << argv[0] << " <node_name> <depth>\n";
        cerr << "Extended Usage: " << argv[0] << " <node_name> <depth> <seq|par> [num_threads]\n";
        return 1;
    }

    
    string start_node = argv[1];     // example "Tom Hanks" 
    int depth;
    
    try {
        depth = stoi(argv[2]);
    } catch (const exception& e) {
        cerr << "Error: Depth must be an integer.\n";
        return 1;
    }
    

    if (argc == 3) {
        // original sequential flow
        CURL* curl = curl_easy_init();
        if (!curl) {
            cerr << "Failed to initialize CURL" << endl;
            return -1;
        }

        const auto start = std::chrono::steady_clock::now();

        for (const auto& node : bfs(curl, start_node, depth))
            cout << "- " << node << "\n";

        const auto finish = std::chrono::steady_clock::now();
        const std::chrono::duration<double> elapsed_seconds = finish - start;
        std::cout << "Time to crawl: "<<elapsed_seconds.count() << "s\n";

        curl_easy_cleanup(curl);
        return 0;
    }

    // argc >= 4 and user wants an explicit mode
    string mode = argv[3];
    if (mode != "seq" && mode != "par") {
        
        cerr << "Mode must be 'seq' or 'par'\n";
        return 1;
    }

    
    // initialize curl globally 
    curl_global_init(CURL_GLOBAL_ALL);

    try {
        const auto start = std::chrono::steady_clock::now();

        vector<string> visited_nodes;

        if (mode == "seq") {
            // sequential 
            CURL* curl = curl_easy_init();
            if (!curl) {
                cerr << "Failed to initialize CURL" << endl;
                curl_global_cleanup();
                return -1;
            }
            visited_nodes = bfs(curl, start_node, depth);
            curl_easy_cleanup(curl);
        } else {
            // parallel
            int threads = 8;
            if (argc >= 5) {
                try { threads = stoi(argv[4]); } catch (...) { threads = 8; }
                if (threads <= 0) threads = 8;
            }
            visited_nodes = bfs_parallel(start_node, depth, threads);
        }

        const auto finish = std::chrono::steady_clock::now();
        const std::chrono::duration<double> elapsed_seconds = finish - start;

        cout << "START_NODE: " << start_node << "\n";
        cout << "MODE: " << mode << (mode=="par" && argc>=5 ? (" (threads=" + string(argv[4]) + ")") : "") << "\n";
        cout << "MAX_DEPTH: " << depth << "\n";
        cout << "VISITED_COUNT: " << visited_nodes.size() << "\n";
        cout << "ELAPSED(s): " << elapsed_seconds.count() << "\n";
        cout << "VISITED_NODES:\n";
        
        
        for (const auto &n : visited_nodes) cout << n << "\n";

    } catch (const ParseException& e) {
        cerr << "JSON parse error: " << e.what() << "\n";
        curl_global_cleanup();
        
        return 1;
    } catch (const exception& e) {
        cerr << "Exception: " << e.what() << "\n";
        curl_global_cleanup();
        
        return 1;
    }
    curl_global_cleanup();
    return 0;
}

