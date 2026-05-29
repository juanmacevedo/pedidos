#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <signal.h>

#define N_PRODUCTORES         2
#define M_COCINEROS           3
#define R_REPARTIDORES        2
#define TAM_COLA_PENDIENTES   5
#define TAM_COLA_LISTOS       5

typedef struct {
    int id;
    int tipo_comida;          /* 1, 2, o 3 */
    int tiempo_preparacion;   /* igual al tipo: 2, 3, o 4 seg */
    int entregado;            /* 0 = pendiente, 1 = entregado */
} Pedido;

typedef struct {
    Pedido items[TAM_COLA_PENDIENTES];
    int frente;
    int fin;
    int cantidad;
} ColaPendientes;

typedef struct {
    Pedido items[TAM_COLA_LISTOS];
    int frente;
    int fin;
    int cantidad;
} ColaListos;

ColaPendientes cola_pendientes;
ColaListos     cola_listos;

int ultimo_id       = 0;
int total_entregados = 0;

void manejador(int sig) {
    printf("\n--- Resumen ---\n");
    printf("Entregados: %d\n", total_entregados);
    exit(0);
}


void encolar_pendientes(ColaPendientes* c, Pedido p) {
    c->items[c->fin] = p;
    c->fin = (c->fin + 1) % TAM_COLA_PENDIENTES;
    c->cantidad++;
}

Pedido desencolar_pendientes(ColaPendientes* c) {
    Pedido p = c->items[c->frente];
    c->frente = (c->frente + 1) % TAM_COLA_PENDIENTES;
    c->cantidad--;
    return p;
}

void encolar_listos(ColaListos* c, Pedido p) {
    c->items[c->fin] = p;
    c->fin = (c->fin + 1) % TAM_COLA_LISTOS;
    c->cantidad++;
}

Pedido desencolar_listos(ColaListos* c) {
    Pedido p = c->items[c->frente];
    c->frente = (c->frente + 1) % TAM_COLA_LISTOS;
    c->cantidad--;
    return p;
}

int generar_id() {
    return ++ultimo_id;
}

void* productor(void* arg) {
    int id = *(int*)arg;
    while (1) {
        Pedido p;
        p.id                 = generar_id();
        unsigned int semilla = (unsigned int)pthread_self();
        p.tipo_comida        = rand_r(&semilla) % 3 + 1;
        p.tiempo_preparacion = p.tipo_comida + 1;
        p.entregado          = 0;
        sleep(1);
        encolar_pendientes(&cola_pendientes, p);
        printf("[PRODUCTOR  %d] Pedido #%d generado — tipo %d (prep: %ds)\n", id, p.id, p.tipo_comida, p.tiempo_preparacion);
    }
    return NULL;
}

void* cocinero(void* arg) {
    int id = *(int*)arg;
    while (1) {
        Pedido p = desencolar_pendientes(&cola_pendientes);
        printf("[COCINERO   %d] Tomó pedido #%d — tipo %d\n", id, p.id, p.tipo_comida);
        sleep(p.tiempo_preparacion);
        encolar_listos(&cola_listos, p);
        printf("[COCINERO   %d] Pedido #%d listo\n", id, p.id);
    }
    return NULL;
}

void* repartidor(void* arg) {
    int id = *(int*)arg;
    while (1) {
        Pedido p = desencolar_listos(&cola_listos);
        sleep(1);
        p.entregado = 1;
        total_entregados++;
        printf("[REPARTIDOR %d] Pedido #%d entregado — tipo %d (total: %d)\n", id, p.id, p.tipo_comida, total_entregados);
    }
    return NULL;
}

int main() {
    signal(SIGINT, manejador);
    

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

    return 0;
}
