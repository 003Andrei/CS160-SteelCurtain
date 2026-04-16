// g++ -std=c++20 -pthread pthread.cpp -o pthread && ./pthread

#include <iostream>
#include <pthread.h>
#include <unistd.h>

struct ThreadArg {
  int id;
};

void *worker(void *arg) {
  ThreadArg *data = static_cast<ThreadArg *>(arg);
  for (int i = 0; i < 5; ++i) {
    std::cout << "pthread worker " << data->id << ", iteration " << i
              << std::endl;
    usleep(200000); // 200 ms
  }
  return nullptr;
}

int main() {
  pthread_t t1, t2;
  ThreadArg arg1{1};
  ThreadArg arg2{2};

  pthread_create(&t1, nullptr, worker, &arg1);
  pthread_create(&t2, nullptr, worker, &arg2);

  pthread_join(t1, nullptr);
  pthread_join(t2, nullptr);

  std::cout << "All pthread workers finished." << std::endl;
  return 0;
}