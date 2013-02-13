#ifndef CPOOL_H
#define CPOOL_H

struct node {
  int sockfd;
  struct node* next;
};

struct conn_pool {
  struct node* head;
  struct node* tail;
  pthread_mutex_t mutex;
  char* host;
  int port;
};

struct conn_pool* new_connection_pool();

int get(struct conn_pool* pool);

void release(struct conn_pool* pool, int sockfd);

int open_connection(struct conn_pool* pool);

void delete_connection_pool(struct conn_pool* pool);

#endif
