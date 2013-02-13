#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/types.h>

#include "cpool.h"

struct conn_pool* new_connection_pool(char* host, int port) {
  struct conn_pool* pool = malloc(sizeof(struct conn_pool));
  pool->head = NULL;
  pool->tail = NULL;
  pool->host = malloc(strlen(host) + 1);
  strcpy(pool->host, host);
  pool->port = port;
  pthread_mutex_init(&(pool->mutex), NULL);
  return pool;
}

void delete_connection_pool(struct conn_pool* pool) {
  while (pool->head != NULL) {
    int sockfd = get(pool);
    close(sockfd);
  }
  pthread_mutex_destroy(&(pool->mutex));
  free(pool->host);
  free(pool);
}

int get(struct conn_pool* pool) {
  int sockfd;
  pthread_mutex_lock(&(pool->mutex));
  if (pool->head != NULL) {
    struct node* node = pool->head;
    pool->head = node->next;
    if (pool->head == NULL) {
      pool->tail = NULL;
    }
    sockfd = node->sockfd;
    free(node);
  } else {
    sockfd = open_connection(pool);
  }
  pthread_mutex_unlock(&(pool->mutex));
  return sockfd;
}

void release(struct conn_pool* pool, int sockfd) {
  pthread_mutex_lock(&(pool->mutex));
  struct node* node = malloc(sizeof(struct node));
  node->sockfd = sockfd;
  node->next = NULL;
  if (pool->head == NULL && pool->tail == NULL) {
    pool->head = node;
    pool->tail = node;
  } else {
    pool->tail->next = node;
    pool->tail = node;
  }
  pthread_mutex_unlock(&(pool->mutex));
}

int open_connection(struct conn_pool* pool) {
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(pool->port);
  if (inet_pton(AF_INET, pool->host, &server_addr.sin_addr) < 0) {
    printf("Error processing the hostname\n");
    return -1;
  }

  int sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0) {
    printf("Failed to create socket\n");
    return -1;
  }

  if (connect(sockfd, (struct sockaddr*) &server_addr, sizeof(server_addr)) < 0) {
    printf("Failed to connect\n");
    return -1;
  }
  return sockfd;
}
