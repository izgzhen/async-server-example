#include <cstdio>
#include <vector>
#include <unordered_map>
#include <queue>
#include <string>
#include <cstring>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <csignal>
#include <fcntl.h>
#include <poll.h>
#include <algorithm>
#include <assert.h>

#define BACKLOG 16 // how many pending connections queue will hold 
#define MAXFN 255 // # of characters in a path
#define N_WORKER_THREADS 32

#define CHECK_RET(x) do { \
  int retval = (x); \
  if (retval != 0) { \
    fprintf(stderr, "Runtime error: %s returned %d at %s:%d\n", #x, retval, __FILE__, __LINE__); \
    exit(-1); \
  } \
} while (0)

#define CHECK_NON_NULL(x) do { \
  if ((x) == NULL) { \
    fprintf(stderr, "%s: %s == NULL at %s:%d\n", strerror(errno), #x,__FILE__, __LINE__); \
    exit(-1); \
  } \
} while (0)

#define CHECK_NON_MINUS_ONE(x) do { \
  int retval = (x); \
  if (retval == -1) { \
    fprintf(stderr, "%s: %s == -1 at %s:%d\n", strerror(errno), #x, __FILE__, __LINE__); \
    exit(-1); \
  } \
} while (0)

#define CHECK_ZERO(x) do { \
  int retval = (x); \
  if (retval != 0) { \
    fprintf(stderr, "%s: %s != 0 at %s:%d\n", strerror(errno), #x, __FILE__, __LINE__); \
    exit(-1); \
  } \
} while (0)

// make a file descriptor non blocking
void Unblock (int fd) {
    int flags = fcntl(fd, F_GETFL);
    CHECK_NON_MINUS_ONE(flags);
    CHECK_NON_MINUS_ONE(fcntl(fd, F_SETFL, flags | O_NONBLOCK));
}

// possible states for a connection
enum State {
    // we are waiting to receive data from the client
    RECV,
    // waiting worker thread to complete file reading task
    LOAD,
    // we are waiting to be able to write data back over the network
    WRITE,
    // the connection is closed
    CLOSED
};

class Connection;

struct ReadFileTask {
    char* filepath;
    int notify_fd;
    Connection* conn;
};

class TaskQueue {
public:
    TaskQueue() {
        CHECK_RET(pthread_mutex_init(&mutex_, NULL));
        CHECK_RET(pthread_cond_init(&cond_, NULL));
    }

    void AddReadFileTask(ReadFileTask *task) {
        CHECK_RET(pthread_mutex_lock(&mutex_));
        tasks_.push(task);
        CHECK_RET(pthread_mutex_unlock(&mutex_));
        CHECK_RET(pthread_cond_signal(&cond_));
    }
    
    ReadFileTask* GetReadFileTask() {
        CHECK_RET(pthread_mutex_lock(&mutex_));

        while (tasks_.empty() && !stopped_) {
            CHECK_RET(pthread_cond_wait(&cond_, &mutex_));
        }
        if (stopped_) {
            CHECK_RET(pthread_mutex_unlock(&mutex_));
            CHECK_RET(pthread_cond_signal(&cond_));
            return NULL;
        }

        ReadFileTask* task = tasks_.front();
        tasks_.pop();

        CHECK_RET(pthread_mutex_unlock(&mutex_));
        return task;
    }

    bool Stopped() { return stopped_; }

    void Stop() {
        stopped_ = true;
        CHECK_RET(pthread_cond_signal(&cond_));
    }

private:
    std::queue<ReadFileTask*> tasks_;
    pthread_mutex_t mutex_;
    pthread_cond_t cond_;
    bool stopped_{false};
};

class Connection {
public:
    Connection (void) {
        state_ = RECV;
    }
    ~Connection() {}

    // establish connection
    void Accept(int server_fd) {
        assert(state_ == RECV);
        struct sockaddr_storage client_addr; // connector's address information
        socklen_t sin_size = sizeof client_addr;

        // accept connection
        sockfd_ = accept(server_fd, (struct sockaddr *)&client_addr, &sin_size);
        CHECK_NON_MINUS_ONE(sockfd_);
        Unblock(sockfd_);
        printf("Accepted client connection on socket %d.\n", sockfd_);
    }

    // receive data
    int Receive(TaskQueue* queue) {
        assert(state_ == RECV);
        printf("Ready to read from connection %d\n", sockfd_);

        int numbytes = recv(sockfd_, buf, MAXFN - 1, 0);
        CHECK_NON_MINUS_ONE(numbytes);
        if (numbytes <= 0 || buf[numbytes - 1] != '\n') {
            fprintf(stderr, "unexpected path input from client: '%s'\n", buf);
            return -1;
        }

        buf[numbytes - 1] = '\0';
        printf("Received '%s'\n", buf);
        int pipefd[2];
        CHECK_NON_MINUS_ONE(pipe(pipefd));
        int read_end = pipefd[0];
        int write_end = pipefd[1];
        
        ReadFileTask* t = new ReadFileTask;
        t->notify_fd = write_end;
        t->filepath = buf;
        t->conn = this;
        queue->AddReadFileTask(t);
        state_ = LOAD;
        return read_end;
    }

    // update state after the file content is loaded into memory
    void Loaded() {
        assert(state_ == LOAD);
        state_ = WRITE;
    }

    // send data to client and close connection
    void Send() {
        assert(state_ == WRITE);
        printf("Sending to connection %d\n", sockfd_);
        int n = send(sockfd_, file_content_, file_length_, 0);
        CHECK_NON_MINUS_ONE(n);
        if (n < file_length_) {
            fprintf(stderr, "send error: %d < %d\n", n, file_length_);
            exit(-1);
        }
        free(file_content_);
    }

    // close connection immediately
    void Close () {
        printf("Closing connection %d\n", sockfd_);
        CHECK_NON_MINUS_ONE(close(sockfd_));
        state_ = CLOSED;
    }

    State GetState() { return state_; }

    int SockFd() { return sockfd_; }

    void SetLoadedFile(char* content, int size) {
        file_content_ = content;
        file_length_ = size;
    }

private:
    // buffer to receive file path
    char buf[MAXFN];
    // state of the connection
    State state_;
    // socket for the client
    int sockfd_;
    // loaded file content
    char* file_content_;
    // loaded file length
    int file_length_;
};

void *ReadFileLoop(void *arg) {
    TaskQueue* queue = (TaskQueue *) arg;
    while (!queue->Stopped()) {
        ReadFileTask* task = queue->GetReadFileTask();
        if (queue->Stopped()) {
            break;
        }
        FILE* f = fopen(task->filepath, "r");
        if (f == NULL) {
            fprintf(stderr, "fopen(%s) failed: %s, closing connection\n", task->filepath, strerror(errno));
            int n = write(task->notify_fd, "er\0", 3);
            CHECK_NON_MINUS_ONE(n);
            delete task;
            continue;
        }
        CHECK_NON_MINUS_ONE(fseek(f, 0, SEEK_END));
        int size = ftell(f);
        CHECK_NON_MINUS_ONE(size);
        rewind(f);
        char* content = (char*) malloc(sizeof(char) * size);
        int n = fread(content, sizeof(char), size, f);
        if (n != size) {
            fprintf(stderr, "fread error: %d != %d\n", n, size);
            exit(-1);
        }
        task->conn->SetLoadedFile(content, size);
        n = write(task->notify_fd, "ok\0", 3);
        CHECK_NON_MINUS_ONE(n);
        CHECK_ZERO(fclose(f));
        delete task;
    }
    return NULL;
}

class ThreadPool {
public:
    ThreadPool(TaskQueue* queue) {
        for (int i = 0; i < N_WORKER_THREADS; i++) {
            CHECK_RET(pthread_create(&threads_[i], NULL, ReadFileLoop, queue));
        }
    }
    void Stop() {
        for (int i = 0; i < N_WORKER_THREADS; i++) {
            printf("stopping worker thread %d\n", i);
            CHECK_ZERO(pthread_join(threads_[i], NULL));
        }
    }
private:
    pthread_t threads_[N_WORKER_THREADS];
};

class Server {
public:
    Server (TaskQueue* queue) : queue_(queue) {
        kill_server_ = false;
    }

    // start server
    void Listen (const char *address, const char *port) {
        // code from http://www.beej.us/guide/bgnet/html/multi/syscalls.html
        int status;
        struct addrinfo hints;
        struct addrinfo *servinfo;  // will point to the results

        memset(&hints, 0, sizeof hints); // make sure the struct is empty
        hints.ai_family = AF_UNSPEC;     // don't care IPv4 or IPv6
        hints.ai_socktype = SOCK_STREAM; // TCP stream sockets
        hints.ai_flags = AI_PASSIVE;     // fill in my IP for me

        if ((status = getaddrinfo(address, port, &hints, &servinfo)) != 0) {
            fprintf(stderr, "getaddrinfo error: %s\n", gai_strerror(status));
            exit(1);
        }

        // loop through all the results and bind to the first we can
        struct addrinfo *p;
        int yes = 1;
        for(p = servinfo; p != NULL; p = p->ai_next) {
            // make a socket
            sockfd_ = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
            CHECK_NON_MINUS_ONE(sockfd_);
            CHECK_NON_MINUS_ONE(setsockopt(sockfd_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)));
            // bind socket to a port
            CHECK_NON_MINUS_ONE(bind(sockfd_, p->ai_addr, p->ai_addrlen));

            Unblock(sockfd_);
            break;
        }
        freeaddrinfo(servinfo);

        if (p == NULL)  {
            printf("server: failed to bind\n");
            Stop();
        }

        // listen
        CHECK_NON_MINUS_ONE(listen(sockfd_, BACKLOG));
        printf("Listening on %s:%s\n", address, port);

        // infinite loop
        while (!kill_server_) {
            Poll();
        }

        // close
        printf("Shutting down ...\n");
        CleanUp();
    }

    // stop server
    void Stop (void) {
        kill_server_ = true;
    }

private:
    // the socket file descriptor used by the server
    int sockfd_;

    // whether we have received a signal to end the server
    bool kill_server_;

    // active connections
    std::unordered_map<int, Connection*> conns_;

    std::unordered_map<int, Connection*> pipe_conns_;

    TaskQueue* queue_;

    // free resources
    void CleanUp (void) {
        close(sockfd_);
    }

    // block until we receive events on file descriptors
    void Poll (void) {
        std::vector<pollfd> polls;

        // sockets of all active connections
        // push these first so the first k items in polls share indices with conns_
        std::vector<int> closed_conns_fds;
        for (auto kv : conns_) {
            Connection* conn = kv.second;
            if (conn->GetState() == CLOSED) {
                delete kv.second;
                closed_conns_fds.push_back(kv.first);
                printf("Closed connection when creating poll: %d\n", conn->SockFd());
                continue; // connection is closed
            }
            if (conn->GetState() == LOAD) {
                continue;
            }
            pollfd fd2;
            fd2.fd = conn->SockFd();
            if (conn->GetState() == RECV) {
                fd2.events = POLLIN;
            } else if (conn->GetState() == WRITE) {
                fd2.events = POLLOUT;
            } else {
                printf("invalid connection state: %d\n", conn->GetState());
                exit(-1);
            }
            polls.push_back(fd2);
        }

        for (int fd : closed_conns_fds) {
            conns_.erase(fd);
        }

        for (auto kv : pipe_conns_) {
            pollfd fd;
            fd.fd = kv.first;
            fd.events = POLLIN;
            polls.push_back(fd);
        }

        // the listening socket
        pollfd fd1;
        fd1.fd = sockfd_;
        fd1.events = POLLIN;
        polls.push_back(fd1);

        // poll to receive receive event notifications
        int res = poll(&polls[0], polls.size(), -1);

        CHECK_NON_MINUS_ONE(res);

        // the call timed out and no file descriptors were ready.
        if (res == 0) {
            return;
        }

        // handle events
        for (size_t i = 0; i < polls.size(); i++) {
            short re = polls[i].revents;
            int fd = polls[i].fd;
            if (re) {
                if (fd == sockfd_ && (re & POLLIN)) {
                    // event happens at the listening socket
                    // accept incoming connection
                    Accept();
                } else if (conns_.find(fd) != conns_.end()) {
                    Connection* conn = conns_[fd];
                    // read or write to client socket
                    if (re & POLLIN) {
                        int read_end = conn->Receive(queue_);
                        if (read_end == -1) {
                          // client side error
                          conn->Close();
                          delete conn;
                          conns_.erase(fd);
                          continue;
                        }
                        Unblock(read_end);
                        pipe_conns_.insert({read_end, conn});
                    } else if (re & POLLOUT) {
                        conn->Send();
                        conn->Close();
                        delete conn;
                        conns_.erase(fd);
                    } else {
                        printf("Error polling client socket %d.\n", conn->SockFd());
                        conn->Close();
                        delete conn;
                        conns_.erase(fd);
                    }
                } else if (pipe_conns_.find(fd) != pipe_conns_.end()) {
                    Connection* conn = pipe_conns_[fd];
                    char buf[3];
                    size_t n = read(fd, buf, 3);
                    CHECK_NON_MINUS_ONE(n);

                    if (strcmp(buf, "er") == 0) {
                        conn->Close();
                    }
                    else if (strcmp(buf, "ok") == 0) {
                        conn->Loaded();
                    } else {
                        fprintf(stderr, "invalid buf");
                        exit(-1);
                    }
                    CHECK_NON_MINUS_ONE(close(fd));
                    pipe_conns_.erase(fd);
                } else {
                    printf("unknown fd: %d\n", fd);
                    exit(-1);
                }
            }
        }
    }

    // accept a client connection
    void Accept (void) {
        Connection *conn = new Connection();
        conn->Accept(sockfd_);
        conns_.insert({conn->SockFd(), conn});
        printf("Active connections: %lu\n", conns_.size());
    }
};

TaskQueue queue;
Server server(&queue);
ThreadPool pool(&queue);

void signalHandler (int signum) {
    printf("Interrupt signal (%d) received.\n", signum);
    queue.Stop();
    server.Stop();
    pool.Stop();
    exit(0);
}

int main (int argc, char *argv[]) {
    if (argc < 3) {
        printf("Usage: 550server [address] [port]\n");
        return -1;
    }

    // prevent SIGPIPE from crashing the server
    struct sigaction sa;
    sa.sa_handler = SIG_IGN;
    sa.sa_flags = 0;
    CHECK_NON_MINUS_ONE(sigaction(SIGPIPE, &sa, 0));

    // register signal handler
    signal(SIGINT, signalHandler); 
    signal(SIGTERM, signalHandler); 


    // start server
    server.Listen(argv[1], argv[2]);

    return 0;
}
