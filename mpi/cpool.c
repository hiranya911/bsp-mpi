#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/types.h>

#include "cpool.h"

struct connection_pool* new_connection_pool() {
  struct connection_pool* pool = malloc(sizeof(struct connection_pool));
  pool->head = NULL;
  pool->tail = NULL;
  pthread_mutex_init(&(pool->mutex), NULL);
  return pool;
}

void delete_connection_pool(struct connection_pool* pool) {
  while (pool->head != NULL) {
    struct connection_list* list = pool->head;
    while (list->head != NULL) {
      int sockfd = get_connection(pool, list->host, list->port);
      close(sockfd);
    }
    pool->head = list->next;
    free(list->host);
    free(list);
  }
  pthread_mutex_destroy(&(pool->mutex));
  free(pool);
}

int get_connection(struct connection_pool* pool, char* host, int port) {
  int sockfd;
  pthread_mutex_lock(&(pool->mutex));
  struct connection_list* list = pool->head;
  while (list != NULL) {
    if (strcmp(list->host, host) == 0 && list->port == port) {
      break;
    } else {
      list = list->next;
    }
  }

  if (list == NULL) {
    list = malloc(sizeof(struct connection_list));
    list->head = NULL;
    list->tail = NULL;
    list->host = malloc(strlen(host) + 1);
    strcpy(list->host, host);
    list->port = port;
    list->next = NULL;
    if (pool->head == NULL) {
      pool->head = list;
      pool->tail = list;
    } else {
      pool->tail->next = list;
      pool->tail = list;
    }
  }

  if (list->head != NULL) {
    struct connection* conn = list->head;
    list->head = conn->next;
    if (list->head == NULL) {
      list->tail = NULL;
    }
    sockfd = conn->sockfd;
    free(conn);
  } else {
    sockfd = open_connection(host, port);
  }
  pthread_mutex_unlock(&(pool->mutex));
  return sockfd;
}

void release_connection(struct connection_pool* pool, char* host, int port, int sockfd) {
  pthread_mutex_lock(&(pool->mutex));
  struct connection_list* list = pool->head;
  while (list != NULL) {
    if (strcmp(list->host, host) == 0 && list->port == port) {
      struct connection* conn = malloc(sizeof(struct connection));
      conn->sockfd = sockfd;
      conn->next = NULL;
      if (list->head == NULL) {
	list->head = conn;
	list->tail = conn;
      } else {
	list->tail->next = conn;
	list->tail = conn;
      }
      break;
    }
    list = list->next;
  }
  pthread_mutex_unlock(&(pool->mutex));
}

int open_connection(char* host, int port) {
  struct sockaddr_in server_addr;
  memset(&server_addr, 0, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  if (inet_pton(AF_INET, host, &server_addr.sin_addr) < 0) {
    printf("Error processing the hostname\n");
    return -1;
  }

  int sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0) {
    printf("Failed to create socket\n");
    return -1;
  }

  if (connect(sockfd, (struct sockaddr*) &server_addr, sizeof(server_addr)) < 0) {
    printf("Failed to connect to %s:%d\n", host, port);
    return -1;
  }
  return sockfd;
}
