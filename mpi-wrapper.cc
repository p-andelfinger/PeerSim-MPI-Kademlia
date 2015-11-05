#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <mpi.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <sys/time.h>
#include <unistd.h>
#include <iostream>
#include <map>
#include <set>
#include <sstream>

#define MAX_MPI_MESSAGE_SIZE 1024
#define MAX_PIPE_MESSAGE_SIZE (MAX_MPI_MESSAGE_SIZE + 2) // includes message length and source

using namespace std;

int32_t count = 0;

int32_t rank;
int32_t size;

int32_t inPipeFd, outPipeFd;

int32_t mpiMessageBuffer[MAX_MPI_MESSAGE_SIZE];
int32_t cToJavaMessageBuffer[MAX_PIPE_MESSAGE_SIZE];
int32_t javaToCMessageBuffer[MAX_PIPE_MESSAGE_SIZE];

bool tryForwardJavaToMpi() {
    int32_t ints = 0;

    int nBytes = read(inPipeFd, &ints, sizeof(int32_t));

    if (nBytes == -1)
        return false;

    if (ints == -1) {
        fprintf(stderr, "+++ %d (wrapper): shutdown is true", rank);
        return true;
    }

    if (ints == 0)
        return false;

    // TODO: can there be incomplete messages in the buffer after this?
    nBytes = read(inPipeFd, javaToCMessageBuffer, ints * sizeof(int32_t));

    if (ints != nBytes / 4) {
        fprintf(stderr, "rank %d: wanted to read %d ints, got %d\n", rank, ints, nBytes / 4);
        for (int i = 0; i < nBytes / 4; i++) {
            fprintf(stderr, "%d ", javaToCMessageBuffer[i]);
        }
        fprintf(stderr, "\n");
    }

    count++;

    int targetRank = javaToCMessageBuffer[0];
    javaToCMessageBuffer[0] = ints - 1;

    int r = MPI_Send(javaToCMessageBuffer, ints, MPI_INT, targetRank, 0, MPI_COMM_WORLD);

    if (r != MPI_SUCCESS) {
        fprintf(stderr, "+++ %d (wrapper): an MPI_Send failed: %d\n", rank, r);
    }

    return false;
}

int inMessageCount = 0;

void tryForwardMpiToJava() {
    int32_t ints;
    int32_t messageWaiting;

    MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &messageWaiting, MPI_STATUS_IGNORE);

    if (messageWaiting) {
        MPI_Status status;
        MPI_Recv(mpiMessageBuffer + 1, MAX_MPI_MESSAGE_SIZE, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        ints = mpiMessageBuffer[1];
        mpiMessageBuffer[0] = ints;
        mpiMessageBuffer[1] = status.MPI_SOURCE;

        if (mpiMessageBuffer[2] == 2)
            fprintf(stderr, "rank %d: dumping message %d of type %d from %d in outpipe, ints was %d, now is %d\n", rank, inMessageCount, mpiMessageBuffer[2], mpiMessageBuffer[1], ints, ints + 1);
        inMessageCount++;
        int length = sizeof(int32_t) * (ints + 2);
        int nBytes;
        do {
            nBytes = write(outPipeFd, mpiMessageBuffer, length);
            if (nBytes == -1) {
                tryForwardJavaToMpi();
            } else if (nBytes != length) {
                fprintf(stderr, "+++ %d (wrapper): something went wrong: write didn't succeed: %d, %d", rank, nBytes, errno);
                exit(1);
            }
        } while (nBytes != length);
    }
}

void sig_handler(int signo)
{
}

int32_t main(int32_t argc, char **argv)
{
    if (signal(SIGPIPE, sig_handler) == SIG_ERR)
        printf("can't catch SIGPIPE\n");

    char javaToCPipeName[128];
    char cToJavaPipeName[128];
    char commandBuf[1024];
    char sizeBuf[128];
    char rankBuf[128];
    char pipeIdBuf[128];
    char heapArgumentBufX[128];
    char heapArgumentBufS[128];
    struct timeval currentTime;

    // TODO: create java process

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    int pid = getpid();

    gettimeofday(&currentTime, NULL);
    unsigned int currentMs  = currentTime.tv_sec * 1000 + currentTime.tv_usec /
        1000;
    sprintf(javaToCPipeName, "pipes/javaToCPipe_%d_%d", pid, currentMs);
    sprintf(cToJavaPipeName, "pipes/cToJavaPipe_%d_%d", pid, currentMs);

    sprintf(commandBuf, "mkfifo %s", javaToCPipeName);
    system(commandBuf);
    sprintf(commandBuf, "mkfifo %s", cToJavaPipeName);
    system(commandBuf);

    sprintf(heapArgumentBufS, "-Xms%sm", argv[1]);
    sprintf(heapArgumentBufX, "-Xmx%sm", argv[1]);

    if (size > 1) {
        inPipeFd = open(javaToCPipeName, O_RDONLY | O_NONBLOCK);
        fprintf(stderr, "+++ %d (wrapper): opened input pipe %s: %d\n", rank,
                cToJavaPipeName, inPipeFd);
    }

    int32_t fpid = fork();
    fprintf(stderr, "pid: %d\n", fpid);

    if (!fpid) {
        sprintf(sizeBuf, "%d", size);
        sprintf(rankBuf, "%d", rank);
        sprintf(pipeIdBuf, "%d_%d", pid, currentMs);
        execlp("java", "java", heapArgumentBufS, heapArgumentBufX, "-jar",
                "PeerSim.jar", "scenario.cfg", sizeBuf, rankBuf, pipeIdBuf,
                NULL);
        return 0;
    }

    if (size > 1) {
        fprintf(stderr, "+++ %d (wrapper): waiting to open output pipe %s\n",
                rank, cToJavaPipeName);
        do {
            outPipeFd = open(cToJavaPipeName, O_WRONLY | O_NONBLOCK);
        } while (outPipeFd == -1);

        fprintf(stderr, "+++ %d (wrapper): got an open output pipe %s\n", rank,
                cToJavaPipeName);

        bool shutdown = false;
        if (size > 1) {
            do {
                tryForwardMpiToJava();
                shutdown = tryForwardJavaToMpi();
            } while (!shutdown);
        }

        fprintf(stderr, "+++ %d (wrapper): closing pipes\n", rank);

        close(inPipeFd);
        close(outPipeFd);

        fprintf(stderr, "+++ %d (wrapper): deleting pipes\n", rank);
    }

    fprintf(stderr, "+++ %d (wrapper): waiting for java thread to exit...\n",
            rank);
    wait(NULL);

    fprintf(stderr, "+++ %d (wrapper): entering MPI_Finalize\n", rank);

    MPI_Finalize();

    return 0;
}
