# Sistema Concurrente de Atención de Pedidos

---

## esqueleto.c
Sin sincronización.
```bash
gcc esqueleto.c -lpthread -o esqueleto
```
```bash
./esqueleto
```

---

## esqueleto_semaforo.c
Semáforos — controla disponibilidad y capacidad de colas.
```bash
gcc esqueleto_semaforo.c -lpthread -o esqueleto_semaforo
```
```bash
./esqueleto_semaforo
```

---

## esqueleto_mutex.c
Mutex — protege acceso simultáneo a colas e IDs.
```bash
gcc esqueleto_mutex.c -lpthread -o esqueleto_mutex
```
```bash
./esqueleto_mutex
```

---

## delivery_system.c
Sistema completo con semáforos y mutex.
```bash
gcc delivery_system.c -lpthread -o delivery_system
```
```bash
./delivery_system
```
