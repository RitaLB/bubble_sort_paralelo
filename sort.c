#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <pthread.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>

void bubble_sort(unsigned int *v, int tam);

void imprime_vet(unsigned int *v, int tam);

int le_vet(char *nome_arquivo, unsigned int *v, int tam);

int sort_paralelo(unsigned int *vetor, unsigned int tam, unsigned int ntasks, unsigned int nthreads);

void set_intervalos(unsigned int *vetor, unsigned int tam, unsigned int ntasks, unsigned int nthreads, int* inicio_intervalos, int* num_por_intervalo);

int informa_intervalo(unsigned int num, unsigned int intervalo_min, unsigned int resto);

typedef struct {
    unsigned int* address;
    int length;
} TaskData;

typedef struct {
    int thread_id;
    TaskData *tasks;    
} ThreadArg;

pthread_mutex_t tasks_mutex;
int task_index = 0;
int total_tasks = 0;

// Funcao de ordenacao fornecida. Não pode alterar.
void bubble_sort(unsigned int *v, int tam){
    int i, j, temp, trocou;

    for(j = 0; j < tam - 1; j++){
        trocou = 0;
        for(i = 0; i < tam - 1; i++){
            if(v[i + 1] < v[i]){
                temp = v[i];
                v[i] = v[i + 1];
                v[i + 1] = temp;
                trocou = 1;
            }
        }
        if(!trocou) break;
    }
}

void *thread_bubble_sort(void *arg) {
    ThreadArg *thread_data = (ThreadArg *)arg;
    int id = thread_data->thread_id;
    TaskData *tasks = thread_data->tasks;

    while (1) {
        pthread_mutex_lock(&tasks_mutex);
        int aux = task_index++;
        pthread_mutex_unlock(&tasks_mutex);

        if (aux >= total_tasks)
            break;

        if (tasks[aux].length < 1)
            break;

        printf("Thread %d processando tarefa %d\n", id, aux);
        bubble_sort(tasks[aux].address, tasks[aux].length);
    }

    return NULL;
}

// Funcao para imprimir um vetor. Não pode alterar.
void imprime_vet(unsigned int *v, int tam) {
    int i;
    for(i = 0; i < tam; i++)
        printf("%d ", v[i]);
    printf("\n");
}

// Funcao para ler os dados de um arquivo e armazenar em um vetor em memoroa. Não pode alterar.
int le_vet(char *nome_arquivo, unsigned int *v, int tam) {
    FILE *arquivo;
    
    // Abre o arquivo
    arquivo = fopen(nome_arquivo, "r");
    if (arquivo == NULL) {
        perror("Erro ao abrir o arquivo");
        return 0;
    }

    // Lê os números
    for (int i = 0; i < tam; i++)
        fscanf(arquivo, "%u", &v[i]);

    fclose(arquivo);

    return 1;
}

int sort_paralelo(unsigned int *vetor, unsigned int tam, unsigned int ntasks, unsigned int nthreads) {
    int* inicio_intervalos = calloc(ntasks, sizeof(int));
    int* num_por_intervalo = calloc(ntasks, sizeof(int)); 
    if (inicio_intervalos == NULL || num_por_intervalo == NULL) {
        fprintf(stderr, "Erro: falha ao alocar memória para intervalos.\n");
        free(inicio_intervalos);
        free(num_por_intervalo);
        return 0;
    }

    set_intervalos(vetor, tam, ntasks, nthreads, inicio_intervalos, num_por_intervalo);

    pthread_t threads[nthreads];
    TaskData *tasks = malloc(ntasks * sizeof(TaskData));
    ThreadArg *thread_args = malloc(nthreads * sizeof(ThreadArg));

    if (tasks == NULL || thread_args == NULL) {
        fprintf(stderr, "Erro: falha ao alocar memória para tarefas ou argumentos das threads.\n");
        free(inicio_intervalos);
        free(num_por_intervalo);
        free(tasks);
        free(thread_args);
        return 0;
    }

    pthread_mutex_init(&tasks_mutex, NULL);
    task_index = 0;
    total_tasks = ntasks;

    for (int i = 0; i < ntasks; i++) {
        tasks[i].address = &vetor[inicio_intervalos[i]];
        tasks[i].length = num_por_intervalo[i];
    }

    for (int i = 0; i < nthreads; i++) {
        thread_args[i].thread_id = i;
        thread_args[i].tasks = tasks;

        pthread_create(&threads[i], NULL, thread_bubble_sort, &thread_args[i]);
    }

    for (int i = 0; i < nthreads; i++) {
        pthread_join(threads[i], NULL);
    }

    free(inicio_intervalos);
    free(num_por_intervalo);
    free(tasks);
    free(thread_args);
    pthread_mutex_destroy(&tasks_mutex);

    return 1;
}

void set_intervalos(unsigned int *vetor, unsigned int tam, unsigned int ntasks, unsigned int nthreads, int* inicio_intervalos, int* num_por_intervalo){
    // 1) Calculo do intervalo da thread:
    int intervalo_min = tam/ntasks;  
    int resto = tam % ntasks; 
    // Numero de itens por thread : min_num_t + (i < rest ? 1 : 0)

    // 2) Calcular número de valores em cada intervalo:
    for (int i = 0; i < tam; i++) {
        int intervalo = informa_intervalo(vetor[i], intervalo_min, resto);
        num_por_intervalo[intervalo]+=1;
    }

    // 3) Descobir início de cada intervalo, baseado no número de ítens em cada intervalo descoberto em 2):
    for (int i = 1; i < ntasks; i++) {
        inicio_intervalos[i] = inicio_intervalos[i - 1] + num_por_intervalo[i - 1];
    }

    // 4) Rearranjo usando vetor temporário
    unsigned int* rearranjado = malloc(tam * sizeof(unsigned int));
    int* pos_intervalo = malloc(ntasks * sizeof(int));
    if (rearranjado == NULL || pos_intervalo == NULL) {
        fprintf(stderr, "Erro: falha ao alocar memória para rearranjo.\n");
        free(rearranjado);
        free(pos_intervalo);
        return;
    }

    memcpy(pos_intervalo, inicio_intervalos, ntasks * sizeof(int));

    for (int i = 0; i < tam; i++) {
        int intervalo = informa_intervalo(vetor[i], intervalo_min, resto);
        int posicao = pos_intervalo[intervalo]++;
        rearranjado[posicao] = vetor[i];
    }

    // Copia de volta para vetor original
    memcpy(vetor, rearranjado, tam * sizeof(unsigned int));

    // Libera recursos temporários
    free(rearranjado);
    free(pos_intervalo);
}

// Função que define qual intervalo determinado número pertence
int informa_intervalo(unsigned int num, unsigned int intervalo_min, unsigned int resto) {
    if (resto == 0) {
        return num / intervalo_min;
    }
    
    int fronteira = resto * (intervalo_min + 1);
    return (num < fronteira) ? 
           num / (intervalo_min + 1) : 
           resto + (num - fronteira) / intervalo_min;
}  

// Funcao principal do programa. Não pode alterar.
int main(int argc, char **argv) {
    
    // Verifica argumentos de entrada
    if (argc != 5) {
        fprintf(stderr, "Uso: %s <input> <nnumbers> <ntasks> <nthreads>\n", argv[0]);
        return 1;
    }

    // Argumentos de entrada
    unsigned int nnumbers = atoi(argv[2]);
    unsigned int ntasks = atoi(argv[3]);
    unsigned int nthreads = atoi(argv[4]);
    
    // Aloca vetor
    unsigned int *vetor = malloc(nnumbers * sizeof(unsigned int));

    // Variaveis de controle de tempo de ordenacao
    struct timeval inicio, fim;

    // Le os numeros do arquivo de entrada
    if (le_vet(argv[1], vetor, nnumbers) == 0)
        return 1;

    // Imprime vetor desordenado
    imprime_vet(vetor, nnumbers);

    // Ordena os numeros considerando ntasks e nthreads
    gettimeofday(&inicio, NULL);
    sort_paralelo(vetor, nnumbers, ntasks, nthreads);
    gettimeofday(&fim, NULL);

    // Imprime vetor ordenado
    imprime_vet(vetor, nnumbers);

    // Desaloca vetor
    free(vetor);

    // Imprime o tempo de ordenacao
    printf("Tempo: %.6f segundos\n", fim.tv_sec - inicio.tv_sec + (fim.tv_usec - inicio.tv_usec) / 1e6);
    
    return 0;
}
