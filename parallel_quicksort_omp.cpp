#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <tuple>
#include <vector>
#include <chrono>
#include <omp.h>
using namespace std;

static inline void insertion_sort(vector<int>& a, int l, int r) {
    for (int i = l + 1; i <= r; ++i) {
        int x = a[i], j = i - 1;
        while (j >= l && a[j] > x) { a[j + 1] = a[j]; --j; }
        a[j + 1] = x;
    }
}
static inline int hoare_partition(vector<int>& a, int l, int r) {
    int pivot = a[(l + r) >> 1];
    int i = l - 1, j = r + 1;
    while (true) {
        do { ++i; } while (a[i] < pivot);
        do { --j; } while (a[j] > pivot);
        if (i >= j) return j;
        swap(a[i], a[j]);
    }
}

void quicksort_serial(vector<int>& a, int l, int r, int cutoff = 32) {
    if (l >= r) return;
    if (r - l + 1 <= cutoff) { insertion_sort(a, l, r); return; }
    int m = hoare_partition(a, l, r);
    quicksort_serial(a, l, m, cutoff);
    quicksort_serial(a, m + 1, r, cutoff);
}

void quicksort_task(vector<int>& a, int l, int r, int depth_left, int cutoff) {
    if (l >= r) return;
    if (r - l + 1 <= cutoff) { insertion_sort(a, l, r); return; }
    int m = hoare_partition(a, l, r);

    if (depth_left > 0) {
        #pragma omp task default(none) firstprivate(l, m, depth_left, cutoff) shared(a)
        { quicksort_task(a, l, m, depth_left - 1, cutoff); }
        #pragma omp task default(none) firstprivate(r, m, depth_left, cutoff) shared(a)
        { quicksort_task(a, m + 1, r, depth_left - 1, cutoff); }
        #pragma omp taskwait
    } else {
        quicksort_serial(a, l, m, cutoff);
        quicksort_serial(a, m + 1, r, cutoff);
    }
}
void quicksort_parallel_omp(vector<int>& a, int threads, int cutoff = 32) {
    int depth = max(1, (int)(2 * ceil(log2((double)max(1, threads)))));
    omp_set_dynamic(0);
    omp_set_num_threads(max(1, threads));
    #pragma omp parallel
    {
        #pragma omp single nowait
        { quicksort_task(a, 0, (int)a.size() - 1, depth, cutoff); }
    }
}

static inline bool check_equal(const vector<int>& x, const vector<int>& y) {
    if (x.size() != y.size()) return false;
    for (size_t i = 0; i < x.size(); ++i) if (x[i] != y[i]) return false;
    return true;
}

template <class F>
static inline double time_sec(F&& f) {
    using clk = chrono::steady_clock;
    auto t0 = clk::now();
    f();
    auto t1 = clk::now();
    return chrono::duration<double>(t1 - t0).count();
}

static inline uint64_t splitmix64(uint64_t& x) {
    uint64_t z = (x += 0x9e3779b97f4a7c15ULL);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
}
vector<int> gen_data(size_t n, uint64_t seed=1) {
    vector<int> a(n);
    uint64_t s = seed;
    for (size_t i = 0; i < n; ++i) a[i] = (int)(splitmix64(s) & 0x7fffffff);
    return a;
}

double median(vector<double> v) {
    sort(v.begin(), v.end());
    size_t m = v.size()/2;
    if (v.size() % 2) return v[m];
    return 0.5*(v[m-1] + v[m]);
}

static inline size_t choose_loops(size_t n) {
    if (n <= 1000)   return 4000;
    if (n <= 5000)   return 1000;
    if (n <= 10000)  return 300;
    if (n <= 100000) return 20;
    return 1;
}

tuple<double,double,double> run_once(size_t n, int threads, int reps=3, int cutoff=32, uint64_t seed=1) {
    vector<double> t_ser, t_par;
    t_ser.reserve(reps); t_par.reserve(reps);
    const size_t loops = choose_loops(n);

    for (int r = 0; r < reps; ++r) {
        auto base = gen_data(n, seed + r);

        {
            auto a1 = base; quicksort_serial(a1, 0, (int)a1.size()-1, cutoff);
            auto a2 = base; quicksort_parallel_omp(a2, threads, cutoff);
            if (!check_equal(a1, a2)) {
                fprintf(stderr, "[ERROR] mismatch detected (n=%zu, threads=%d)\n", n, threads);
                exit(2);
            }
        }

        double ts = time_sec([&]{
            for (size_t k = 0; k < loops; ++k) {
                auto a = base;
                quicksort_serial(a, 0, (int)a.size()-1, cutoff);
            }
        }) / (double)loops;

        double tp = time_sec([&]{
            for (size_t k = 0; k < loops; ++k) {
                auto a = base;
                quicksort_parallel_omp(a, threads, cutoff);
            }
        }) / (double)loops;

        t_ser.push_back(ts);
        t_par.push_back(tp);
    }

    double s = median(t_ser);
    double p = median(t_par);
    const double eps = 1e-18;
    return { s*1e6, p*1e6, (s+eps)/(p+eps) };
}

int main(int argc, char** argv) {
    ios::sync_with_stdio(false);
    cin.tie(nullptr);

    if (argc >= 3) {
        size_t n = (size_t)stoull(argv[1]);
        int threads = stoi(argv[2]);
        int reps = (argc >= 4 ? stoi(argv[3]) : 3);
        int cutoff = (argc >= 5 ? stoi(argv[4]) : 32);

        auto [ts, tp, sp] = run_once(n, threads, reps, cutoff);
        cout << "N=" << n << " threads=" << threads
             << " reps=" << reps << " cutoff=" << cutoff
             << " loops=" << choose_loops(n) << "\n";
        cout << fixed << setprecision(3)
             << "serial(us)=" << ts << " parallel(us)=" << tp
             << " speedup=" << sp << "\n";
        return 0;
    }

    vector<size_t> sizes = {1000, 5000, 10000, 100000};
    vector<int>    thrs  = {1, 2, 4, 8};
    int reps = 3, cutoff = 32;

    cout << "# Parallel QuickSort (OpenMP tasks)\n";
    cout << "# reps=" << reps << " cutoff=" << cutoff << "\n";
    cout << left << setw(10) << "N"
         << setw(10) << "threads"
         << setw(16) << "serial(us)"
         << setw(16) << "parallel(us)"
         << setw(12) << "speedup" << "\n";

    cout << fixed << setprecision(3);
    for (auto n : sizes) {
        for (auto t : thrs) {
            auto [ts, tp, sp] = run_once(n, t, reps, cutoff);
            cout << left << setw(10) << n
                 << setw(10) << t
                 << setw(16) << ts
                 << setw(16) << tp
                 << setw(12) << sp
                 << "\n";
        }
    }
    return 0;
}
