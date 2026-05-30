#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>
#include <signal.h>

#define N_PRODUCTORES  2
#define M_COCINEROS    3
#define R_REPARTIDORES 2
#define TAM_COLA       5

typedef struct {
    int id;
    int tipo_comida;
    int tiempo_preparacion;
    int entregado;            /* 0 = pendiente, 1 = entregado */
} Pedido;

typedef struct {
    Pedido items[TAM_COLA];
    int frente;
    int fin;
    int cantidad;
    pthread_mutex_t mutex;
    sem_t lugares;  /* espacios libres */
    sem_t pedidos;  /* pedidos disponibles */
} Cola;

Cola cola_pendientes;
Cola cola_listos;

int ultimo_id        = 0;
int total_entregados = 0;

pthread_mutex_t mutex_id         = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_entregados = PTHREAD_MUTEX_INITIALIZER;

void manejador(int sig) {
    printf("\n--- Resumen ---\n");
    printf("Entregados: %d\n", total_entregados);
    exit(0);
}

void init_cola(Cola* c) {
    c->frente = c->fin = c->cantidad = 0;
    pthread_mutex_init(&c->mutex, NULL);
    sem_init(&c->lugares, 0, TAM_COLA);
    sem_init(&c->pedidos, 0, 0);
}

void destroy_cola(Cola* c) {
    pthread_mutex_destroy(&c->mutex);
    sem_destroy(&c->lugares);
    sem_destroy(&c->pedidos);
}

void encolar(Cola* c, Pedido p) {
    sem_wait(&c->lugares);
    pthread_mutex_lock(&c->mutex);
    c->items[c->fin] = p;
    c->fin = (c->fin + 1) % TAM_COLA;
    c->cantidad++;
    pthread_mutex_unlock(&c->mutex);
    sem_post(&c->pedidos);
}

Pedido desencolar(Cola* c) {
    sem_wait(&c->pedidos);
    pthread_mutex_lock(&c->mutex);
    Pedido p = c->items[c->frente];
    c->frente = (c->frente + 1) % TAM_COLA;
    c->cantidad--;
    pthread_mutex_unlock(&c->mutex);
    sem_post(&c->lugares);
    return p;
}

int generar_id() {
    pthread_mutex_lock(&mutex_id);
    int id = ++ultimo_id;
    pthread_mutex_unlock(&mutex_id);
    return id;
}

void* productor(void* arg) {
    int id = *(int*)arg;
    while (1) {
        Pedido p;
        unsigned int semilla = (unsigned int)pthread_self() ^ (unsigned int)time(NULL);
        p.id                 = generar_id();
        p.tiempo_preparacion = rand_r(&semilla) % 3 + 3;  /* 3, 4, o 5 seg */
        p.tipo_comida        = p.tiempo_preparacion + 10;  /* 13, 14, o 15 */
        p.entregado          = 0;
        sleep(2);
        encolar(&cola_pendientes, p);
        printf("[PRODUCTOR  %d] Pedido #%d generado — tipo %d (preparacion: %ds)\n", id, p.id, p.tipo_comida, p.tiempo_preparacion);
    }
    return NULL;
}

void* cocinero(void* arg) {
    int id = *(int*)arg;
    while (1) {
        Pedido p = desencolar(&cola_pendientes);
        printf("[COCINERO   %d] Tomó pedido #%d — tipo %d\n", id, p.id, p.tipo_comida);
        sleep(p.tiempo_preparacion);
        encolar(&cola_listos, p);
        printf("[COCINERO   %d] Pedido #%d listo\n", id, p.id);
    }
    return NULL;
}

void* repartidor(void* arg) {
    int id = *(int*)arg;
    while (1) {
        Pedido p = desencolar(&cola_listos);
        sleep(2);
        p.entregado = 1;
        pthread_mutex_lock(&mutex_entregados);
        total_entregados++;
        printf("[REPARTIDOR %d] Pedido #%d entregado — tipo %d (total: %d)\n", id, p.id, p.tipo_comida, total_entregados);
        pthread_mutex_unlock(&mutex_entregados);
    }
    return NULL;
}

int main() {
    signal(SIGINT, manejador);

    init_cola(&cola_pendientes);
    init_cola(&cola_listos);

    int ids_prod[N_PRODUCTORES];
    int ids_coc[M_COCINEROS];
    int ids_rep[R_REPARTIDORES];

    pthread_t hilos_prod[N_PRODUCTORES];
    pthread_t hilos_coc[M_COCINEROS];
    pthread_t hilos_rep[R_REPARTIDORES];

    for (int i = 0; i < N_PRODUCTORES; i++) {
        ids_prod[i] = i + 1;
        pthread_create(&hilos_prod[i], NULL, productor, &ids_prod[i]);
    }
    for (int i = 0; i < M_COCINEROS; i++) {
        ids_coc[i] = i + 1;
        pthread_create(&hilos_coc[i], NULL, cocinero, &ids_coc[i]);
    }
    for (int i = 0; i < R_REPARTIDORES; i++) {
        ids_rep[i] = i + 1;
        pthread_create(&hilos_rep[i], NULL, repartidor, &ids_rep[i]);
    }

    for (int i = 0; i < N_PRODUCTORES;  i++) pthread_join(hilos_prod[i], NULL);
    for (int i = 0; i < M_COCINEROS;    i++) pthread_join(hilos_coc[i],  NULL);
    for (int i = 0; i < R_REPARTIDORES; i++) pthread_join(hilos_rep[i],  NULL);

    destroy_cola(&cola_pendientes);
    destroy_cola(&cola_listos);

    return 0;
}
