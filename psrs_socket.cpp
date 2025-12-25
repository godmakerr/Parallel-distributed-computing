#define _WIN32_WINNT 0x0600
#define WIN32_LEAN_AND_MEAN
#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <string>
#include <tuple>
#include <vector>

#ifdef _WIN32
  #include <winsock2.h>
  #include <ws2tcpip.h>
  #pragma comment(lib, "Ws2_32.lib")
  using sock_t = SOCKET;
  static inline void socket_init() { WSADATA w; WSAStartup(MAKEWORD(2,2), &w); }
  static inline void socket_cleanup() { WSACleanup(); }
  static inline void socket_close(sock_t s) { closesocket(s); }
  static inline int  socket_err() { return WSAGetLastError(); }
#else
  #include <arpa/inet.h>
  #include <netinet/in.h>
  #include <sys/socket.h>
  #include <sys/types.h>
  #include <unistd.h>
  using sock_t = int;
  static inline void socket_init() {}
  static inline void socket_cleanup() {}
  static inline void socket_close(sock_t s) { close(s); }
  static inline int  socket_err() { return errno; }
  #define INVALID_SOCKET (-1)
  #define SOCKET_ERROR   (-1)
#endif

using namespace std;

static inline void insertion_sort(vector<int>& a, int l, int r) {
    for (int i = l + 1; i <= r; ++i) {
        int x = a[i], j = i - 1;
        while (j >= l && a[j] > x) { a[j+1] = a[j]; --j; }
        a[j+1] = x;
    }
}
static inline int hoare_partition(vector<int>& a, int l, int r) {
    int mid = (l + r) >> 1;
    int pivot = a[mid];
    int i = l - 1, j = r + 1;
    while (true) {
        do { ++i; } while (a[i] < pivot);
        do { --j; } while (a[j] > pivot);
        if (i >= j) return j;
        swap(a[i], a[j]);
    }
}
static void quicksort_serial(vector<int>& a, int l, int r, int cutoff = 64) {
    if (l >= r) return;
    if (r - l + 1 <= cutoff) { insertion_sort(a, l, r); return; }
    int m = hoare_partition(a, l, r);
    quicksort_serial(a, l, m, cutoff);
    quicksort_serial(a, m + 1, r, cutoff);
}
static inline uint64_t splitmix64(uint64_t& x) {
    uint64_t z = (x += 0x9e3779b97f4a7c15ULL);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
}
static vector<int> gen_data(size_t n, uint64_t seed=1) {
    vector<int> a(n);
    uint64_t s = seed;
    for (size_t i = 0; i < n; ++i) a[i] = (int)(splitmix64(s) & 0x7fffffff);
    return a;
}
template<class T>
static T median(vector<T> v) {
    sort(v.begin(), v.end());
    size_t m = v.size()/2;
    if (v.size()%2) return v[m];
    return (T)((v[m-1] + v[m]) / (T)2);
}

static bool send_all(sock_t s, const void* buf, size_t len) {
    const char* p = (const char*)buf;
    size_t sent = 0;
    while (sent < len) {
        int n = send(s, p + sent, (int)min<size_t>(len - sent, 1<<20), 0);
        if (n == SOCKET_ERROR || n == 0) return false;
        sent += (size_t)n;
    }
    return true;
}
static bool recv_all(sock_t s, void* buf, size_t len) {
    char* p = (char*)buf;
    size_t got = 0;
    while (got < len) {
        int n = recv(s, p + got, (int)min<size_t>(len - got, 1<<20), 0);
        if (n == SOCKET_ERROR || n == 0) return false;
        got += (size_t)n;
    }
    return true;
}
static bool send_int(sock_t s, int32_t v) {
    int32_t net = htonl(v);
    return send_all(s, &net, sizeof(net));
}
static bool recv_int(sock_t s, int32_t& v) {
    int32_t net;
    if (!recv_all(s, &net, sizeof(net))) return false;
    v = (int32_t)ntohl(net);
    return true;
}
static bool send_vec(sock_t s, const vector<int>& a) {
    if (!send_int(s, (int32_t)a.size())) return false;
    if (!a.empty()) {
        vector<int32_t> tmp(a.begin(), a.end());
        for (auto& x : tmp) x = (int32_t)htonl(x);
        return send_all(s, tmp.data(), tmp.size() * sizeof(int32_t));
    }
    return true;
}
static bool recv_vec(sock_t s, vector<int>& a) {
    int32_t n=0;
    if (!recv_int(s, n)) return false;
    if (n < 0) return false;
    a.resize((size_t)n);
    if (n > 0) {
        vector<int32_t> tmp((size_t)n);
        if (!recv_all(s, tmp.data(), tmp.size()*sizeof(int32_t))) return false;
        for (int i=0;i<n;++i) a[i] = (int)ntohl(tmp[i]);
    }
    return true;
}


struct Buckets {
    vector<int> counts;  
    vector<int> concat;  
};

static Buckets make_buckets(const vector<int>& arr, const vector<int>& pivots) {
    int p = (int)pivots.size() + 1;
    Buckets B;
    B.counts.assign(p, 0);
    vector<int> idx(p+1, 0);
    idx[0] = 0;
    for (int i = 0; i < p-1; ++i) {
        idx[i+1] = (int)(upper_bound(arr.begin(), arr.end(), pivots[i]) - arr.begin());
    }
    idx[p] = (int)arr.size();
    int total = 0;
    for (int i = 0; i < p; ++i) {
        int c = idx[i+1] - idx[i];
        B.counts[i] = c;
        total += c;
    }
    B.concat.resize(total);
    int off = 0;
    for (int i = 0; i < p; ++i) {
        int c = B.counts[i];
        if (c > 0) {
            memcpy(&B.concat[off], &arr[idx[i]], c * sizeof(int));
            off += c;
        }
    }
    return B;
}

static bool send_buckets(sock_t s, const Buckets& B) {
    if (!send_int(s, (int32_t)B.counts.size())) return false;
    if (!B.counts.empty()) {
        vector<int32_t> tmp(B.counts.size());
        for (size_t i=0;i<B.counts.size();++i) tmp[i] = htonl((int32_t)B.counts[i]);
        if (!send_all(s, tmp.data(), tmp.size()*sizeof(int32_t))) return false;
    }
    return send_vec(s, B.concat);
}
static bool recv_buckets(sock_t s, Buckets& B) {
    int32_t pc=0;
    if (!recv_int(s, pc)) return false;
    if (pc <= 0) return false;
    B.counts.resize(pc);
    {
        vector<int32_t> tmp(pc);
        if (!recv_all(s, tmp.data(), tmp.size()*sizeof(int32_t))) return false;
        for (int i=0;i<pc;++i) B.counts[i] = (int)ntohl(tmp[i]);
    }
    if (!recv_vec(s, B.concat)) return false;
    int sum = accumulate(B.counts.begin(), B.counts.end(), 0);
    if ((int)B.concat.size() != sum) return false;
    return true;
}

static vector<int> extract_bucket_k(const Buckets& B, int k) {
    int off=0;
    for (int i=0;i<k;++i) off += B.counts[i];
    int len = B.counts[k];
    vector<int> out(len);
    if (len>0) memcpy(out.data(), &B.concat[off], len*sizeof(int));
    return out;
}

int run_master(const string& host, uint16_t port, int workers, size_t N, int reps) {
    socket_init();
    sock_t ls = socket(AF_INET, SOCK_STREAM, 0);
    if (ls == INVALID_SOCKET) { cerr << "socket() failed\n"; return 1; }
    int yes = 1; setsockopt(ls, SOL_SOCKET, SO_REUSEADDR, (char*)&yes, sizeof(yes));
    sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_port = htons(port);
    addr.sin_addr.s_addr = (host == "0.0.0.0") ? INADDR_ANY : inet_addr(host.c_str());
    
    if (bind(ls, (sockaddr*)&addr, sizeof(addr)) == SOCKET_ERROR) {
        cerr << "bind() failed (" << socket_err() << ")\n"; return 2;
    }
    if (listen(ls, workers) == SOCKET_ERROR) { cerr << "listen() failed\n"; return 3; }

    cout << "[master] listening on " << host << ":" << port
         << ", waiting " << workers << " workers...\n";

    vector<sock_t> cli(workers);
    for (int i = 0; i < workers; ++i) {
        sockaddr_in cliaddr{}; socklen_t len = sizeof(cliaddr);
        sock_t cs = accept(ls, (sockaddr*)&cliaddr, &len);
        if (cs == INVALID_SOCKET) { cerr << "accept failed\n"; return 4; }
        cli[i] = cs;
        cout << "[master] worker #" << i + 1 << " connected\n";
    }

    int p = workers + 1;
    for (int i = 0; i < workers; ++i) {
        if (!send_int(cli[i], p) || !send_int(cli[i], i + 1)) { 
            cerr << "send p/id failed\n"; return 5; 
        }
    }

    vector<double> times;
    times.reserve(reps);

    int loops_para = 1;
    if (N <= 1000) loops_para = 500;      // 1K数据跑500次
    else if (N <= 5000) loops_para = 100; // 5K数据跑100次
    else if (N <= 10000) loops_para = 20; // 10K数据跑20次
    else loops_para = 1;                  // 100K数据跑1次

    for (int r = 0; r < reps; ++r) {
        cout << "[master] PSRS run " << (r + 1) << "/" << reps 
             << " (loops=" << loops_para << ") ... ";
        cout.flush();

        auto A_base = gen_data(N, 1 + r);
        
        vector<int> counts(p), displs(p);
        int base = (int)(N / p), rem = (int)(N % p);
        for (int i = 0; i < p; ++i) counts[i] = base + (i < rem ? 1 : 0);
        displs[0] = 0; for (int i = 1; i < p; ++i) displs[i] = displs[i - 1] + counts[i - 1];

        auto t0 = chrono::steady_clock::now();

        for (int k = 0; k < loops_para; ++k) {
            
            vector<int> local(counts[0]);
            if (counts[0] > 0) {
                memcpy(local.data(), &A_base[0], counts[0] * sizeof(int));
            }

            for (int i = 0; i < workers; ++i) {
                int id = i + 1;
                vector<int> chunk(counts[id]);
                if (counts[id] > 0) {
                    memcpy(chunk.data(), &A_base[displs[id]], counts[id] * sizeof(int));
                }
                if (!send_vec(cli[i], chunk)) { cerr << "send chunk failed\n"; return 6; }
            }

            quicksort_serial(local, 0, (int)local.size() - 1, 64);

            int s = max(0, p - 1);
            vector<int> my_samples(s);
            if (s > 0 && !local.empty()) {
                for (int i = 0; i < s; ++i) {
                    size_t idx = (size_t)((i + 1) * (double)local.size() / (double)p);
                    if (idx >= local.size()) idx = local.size() - 1;
                    my_samples[i] = local[idx];
                }
            }

            vector<vector<int>> samples_from(workers);
            for (int i = 0; i < workers; ++i) {
                if (!recv_vec(cli[i], samples_from[i])) { cerr << "recv samples failed\n"; return 7; }
            }

            vector<int> all; all.reserve(s * p);
            if (s > 0) {
                all.insert(all.end(), my_samples.begin(), my_samples.end());
                for (int i = 0; i < workers; ++i) 
                    all.insert(all.end(), samples_from[i].begin(), samples_from[i].end());
                sort(all.begin(), all.end());
            }
            vector<int> pivots(max(0, p - 1), 0);
            if (s > 0) {
                for (int i = 0; i < p - 1; ++i) {
                    int pos = (i + 1) * p - 1;
                    if (pos >= (int)all.size()) pos = (int)all.size() - 1;
                    pivots[i] = all[pos];
                }
            }

            for (int i = 0; i < workers; ++i) {
                if (!send_vec(cli[i], pivots)) { cerr << "send pivots failed\n"; return 8; }
            }

            Buckets myB = make_buckets(local, pivots);

            vector<Buckets> wB(workers);
            for (int i = 0; i < workers; ++i) {
                if (!recv_buckets(cli[i], wB[i])) { cerr << "recv buckets failed\n"; return 9; }
            }
            vector<vector<int>> Final(p);
            for (int k = 0; k < p; ++k) {
                auto bk = extract_bucket_k(myB, k);
                Final[k].insert(Final[k].end(), bk.begin(), bk.end());
            }
            for (int i = 0; i < workers; ++i) {
                for (int k = 0; k < p; ++k) {
                    auto bk = extract_bucket_k(wB[i], k);
                    Final[k].insert(Final[k].end(), bk.begin(), bk.end());
                }
            }

            for (int i = 0; i < workers; ++i) {
                int id = i + 1;
                if (!send_vec(cli[i], Final[id])) { cerr << "send final slice failed\n"; return 10; }
            }
            vector<int> final_local = move(Final[0]);
            quicksort_serial(final_local, 0, (int)final_local.size() - 1, 64);

            for (int i = 0; i < workers; ++i) {
                int32_t ok = 0; 
                if (!recv_int(cli[i], ok) || ok != 1) { cerr << "ACK failed\n"; return 11; }
            }
        } 

        auto t1 = chrono::steady_clock::now();
        double total_sec = chrono::duration<double>(t1 - t0).count();
        double avg_sec = total_sec / loops_para;
        times.push_back(avg_sec);

        cout << "done. avg time=" << fixed << setprecision(9) << avg_sec << " s\n";
    }

    double Tpsrs = median(times);
    cout << "--------------------------------------------------\n";
    cout << "PSRS Median Tp = " << fixed << setprecision(9) << Tpsrs << " s\n";

    vector<double> T1s; 
    T1s.reserve(reps);

    int loop_count = 1;
    if (N <= 1000) loop_count = 2000;
    else if (N <= 5000) loop_count = 500;
    else if (N <= 10000) loop_count = 100;
    else loop_count = 1;

    cout << "[master] Running Serial Benchmark (" << loop_count << " loops/rep)...\n";

    for (int r = 0; r < reps; ++r) {
        auto base_data = gen_data(N, 1 + r);
        
        auto t0 = chrono::steady_clock::now();
        for (int k = 0; k < loop_count; ++k) {
            vector<int> A = base_data;
            quicksort_serial(A, 0, (int)A.size() - 1, 64);
        }
        auto t1 = chrono::steady_clock::now();
        
        double total_time = chrono::duration<double>(t1 - t0).count();
        T1s.push_back(total_time / loop_count);
    }

    double T1 = median(T1s);
    double speedup = (Tpsrs > 1e-9) ? (T1 / Tpsrs) : 0.0;

    cout << "Serial Median T1 = " << fixed << setprecision(9) << T1 << " s\n";
    cout << "Speedup (T1/Tp)  = " << fixed << setprecision(4) << speedup << "\n";
    cout << "--------------------------------------------------\n";

    for (int i = 0; i < workers; ++i) { 
        send_int(cli[i], -1); 
    }

    for (auto s : cli) socket_close(s);
    socket_close(ls);
    socket_cleanup();
    return 0;
}

int run_worker(const string& host, uint16_t port) {
    socket_init();

    sock_t s = socket(AF_INET, SOCK_STREAM, 0);
    if (s == INVALID_SOCKET) { cerr << "socket() failed\n"; return 1; }
    sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_port = htons(port);
    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        cerr << "inet_pton failed\n"; return 2;
    }
    if (connect(s, (sockaddr*)&addr, sizeof(addr)) == SOCKET_ERROR) {
        cerr << "connect failed\n"; return 3;
    }

    int32_t p=0, id=0;
    if (!recv_int(s, p) || !recv_int(s, id)) { cerr << "recv p/id failed\n"; return 4; }
    
    while (true) {
        int32_t n = 0;
        if (!recv_int(s, n)) { 
            break; 
        }

        if (n == -1) {
            cout << "[worker] received exit signal.\n";
            break; 
        }

        vector<int> local;
        if (n > 0) {
            local.resize(n);
            vector<int32_t> tmp(n);
            if (!recv_all(s, tmp.data(), tmp.size() * sizeof(int32_t))) {
                cerr << "recv chunk data failed\n"; 
                return 5; 
            }
            for (int i = 0; i < n; ++i) local[i] = (int)ntohl(tmp[i]);
        } 
        
        quicksort_serial(local, 0, (int)local.size()-1, 64);

        int s_cnt = max(0, p-1);
        vector<int> samples(s_cnt);
        if (s_cnt>0) {
            for (int i=0;i<s_cnt;++i) {
                size_t idx = (size_t)((i+1) * (double)local.size() / (double)p);
                if (idx >= local.size()) idx = local.size()-1;
                samples[i] = local[idx];
            }
        }
        if (!send_vec(s, samples)) { cerr << "send samples failed\n"; return 7; }

        vector<int> pivots;
        if (!recv_vec(s, pivots)) { cerr << "recv pivots failed\n"; return 8; }

        Buckets B = make_buckets(local, pivots);
        if (!send_buckets(s, B)) { cerr << "send buckets failed\n"; return 9; }

        vector<int> final_slice;
        if (!recv_vec(s, final_slice)) { cerr << "recv final slice failed\n"; return 10; }
        if (!final_slice.empty())
            quicksort_serial(final_slice, 0, (int)final_slice.size()-1, 64);
        if (!send_int(s, 1)) { cerr << "send ACK failed\n"; return 11; }

    }

    socket_close(s);
    socket_cleanup();
    return 0;
}

int main(int argc, char** argv) {
    if (argc < 2) {
        cerr << "Usage:\n"
             << "  master <bind_ip> <port> <num_workers> <N> <reps>\n"
             << "  worker <master_ip> <port>\n";
        return 1;
    }
    string mode = argv[1];
    if (mode == "master") {
        if (argc < 7) { cerr << "master args: <bind_ip> <port> <num_workers> <N> <reps>\n"; return 1; }
        string ip = argv[2];
        uint16_t port = (uint16_t)stoi(argv[3]);
        int workers = stoi(argv[4]);
        size_t N = (size_t)stoull(argv[5]);
        int reps = stoi(argv[6]);
        return run_master(ip, port, workers, N, reps);
    } else if (mode == "worker") {
        if (argc < 4) { cerr << "worker args: <master_ip> <port>\n"; return 1; }
        string ip = argv[2];
        uint16_t port = (uint16_t)stoi(argv[3]);
        return run_worker(ip, port);
    } else {
        cerr << "unknown mode\n";
        return 1;
    }
}
