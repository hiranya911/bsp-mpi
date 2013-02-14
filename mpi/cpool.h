#ifndef CPOOL_H
#define CPOOL_H

struct connection {
  int sockfd;
  struct connection* next;
};

struct connection_list {
  struct connection* head;
  struct connection* tail;
  char* host;
  int port;
  struct connection_list* next;
};

struct connection_pool {
  struct connection_list* head;
  struct connection_list* tail;
  pthread_mutex_t mutex;
};

struct connection_pool* new_connection_pool();
int get_connection(struct connection_pool* pool, char* host, int port);
void release_connection(struct connection_pool* pool, char* host, int port, int sockfd);
int open_connection(char* host, int port);
void delete_connection_pool(struct connection_pool* pool);

#endif
