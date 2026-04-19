/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Full implementation:
 *   - UNIX domain socket control-plane IPC (Path B)
 *   - Pipe-based container stdout/stderr capture (Path A)
 *   - Bounded-buffer producer/consumer logging pipeline
 *   - clone() with PID, UTS, and mount namespace isolation
 *   - chroot + /proc mount inside each container
 *   - SIGCHLD, SIGINT, SIGTERM handling
 *   - Soft/hard memory limit registration via ioctl
 *   - Correct zombie reaping and metadata tracking
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

/* ------------------------------------------------------------------ */
/* Constants                                                           */
/* ------------------------------------------------------------------ */

#define STACK_SIZE           (1024 * 1024)
#define CONTAINER_ID_LEN     32
#define CONTROL_PATH         "/tmp/mini_runtime.sock"
#define LOG_DIR              "logs"
#define CONTROL_MESSAGE_LEN  512
#define CHILD_COMMAND_LEN    256
#define LOG_CHUNK_SIZE       4096
#define LOG_BUFFER_CAPACITY  64
#define DEFAULT_SOFT_LIMIT   (40UL << 20)   /* 40 MiB */
#define DEFAULT_HARD_LIMIT   (64UL << 20)   /* 64 MiB */
#define MAX_CONTAINERS       64

/* ------------------------------------------------------------------ */
/* Type Definitions                                                    */
/* ------------------------------------------------------------------ */

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef enum {
    STOP_REASON_NONE = 0,
    STOP_REASON_NORMAL,
    STOP_REASON_STOPPED,
    STOP_REASON_HARD_LIMIT
} stop_reason_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    int stop_requested;      /* set before sending SIGTERM/SIGKILL from stop */
    stop_reason_t stop_reason;
    char log_path[PATH_MAX];
    int log_write_fd;        /* write-end of the log pipe */
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t  not_empty;
    pthread_cond_t  not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
    int is_run;   /* 1 if CMD_RUN (client blocks until exit) */
} control_request_t;

typedef struct {
    int  status;
    int  exit_code;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int  nice_value;
    int  log_write_fd;   /* write-end pipe handed to child */
} child_config_t;

/* ------------------------------------------------------------------ */
/* Producer thread argument                                            */
/* ------------------------------------------------------------------ */
typedef struct {
    int             pipe_read_fd;
    char            container_id[CONTAINER_ID_LEN];
    bounded_buffer_t *buffer;
} producer_arg_t;

/* ------------------------------------------------------------------ */
/* Supervisor context (singleton)                                      */
/* ------------------------------------------------------------------ */
typedef struct {
    int server_fd;
    int monitor_fd;
    volatile int should_stop;
    bounded_buffer_t log_buffer;
    pthread_t        consumer_thread;
    pthread_mutex_t  metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

static supervisor_ctx_t g_ctx;

/* ------------------------------------------------------------------ */
/* Utilities                                                           */
/* ------------------------------------------------------------------ */

static void usage(const char *prog)
{
    fprintf(stderr,
        "Usage:\n"
        "  %s supervisor <base-rootfs>\n"
        "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
        "  %s run   <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
        "  %s ps\n"
        "  %s logs <id>\n"
        "  %s stop <id>\n",
        prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag, const char *value,
                           unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;
    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }
    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }
    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                 int argc, char *argv[], int start_index)
{
    int i;
    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;
        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }
        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i+1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }
        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i+1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }
        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i+1], &end, 10);
            if (errno != 0 || end == argv[i+1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr, "Invalid value for --nice (expected -20..19): %s\n", argv[i+1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }
        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }
    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }
    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING: return "starting";
    case CONTAINER_RUNNING:  return "running";
    case CONTAINER_STOPPED:  return "stopped";
    case CONTAINER_KILLED:   return "killed";
    case CONTAINER_EXITED:   return "exited";
    default:                 return "unknown";
    }
}

static const char *reason_to_string(stop_reason_t r)
{
    switch (r) {
    case STOP_REASON_STOPPED:    return "stopped";
    case STOP_REASON_HARD_LIMIT: return "hard_limit_killed";
    case STOP_REASON_NORMAL:     return "normal";
    default:                     return "none";
    }
}

/* ------------------------------------------------------------------ */
/* Bounded Buffer                                                      */
/* ------------------------------------------------------------------ */

static int bounded_buffer_init(bounded_buffer_t *buf)
{
    int rc;
    memset(buf, 0, sizeof(*buf));
    rc = pthread_mutex_init(&buf->mutex, NULL);
    if (rc) return rc;
    rc = pthread_cond_init(&buf->not_empty, NULL);
    if (rc) { pthread_mutex_destroy(&buf->mutex); return rc; }
    rc = pthread_cond_init(&buf->not_full, NULL);
    if (rc) { pthread_cond_destroy(&buf->not_empty); pthread_mutex_destroy(&buf->mutex); return rc; }
    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buf)
{
    pthread_cond_destroy(&buf->not_full);
    pthread_cond_destroy(&buf->not_empty);
    pthread_mutex_destroy(&buf->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buf)
{
    pthread_mutex_lock(&buf->mutex);
    buf->shutting_down = 1;
    pthread_cond_broadcast(&buf->not_empty);
    pthread_cond_broadcast(&buf->not_full);
    pthread_mutex_unlock(&buf->mutex);
}

/*
 * bounded_buffer_push — producer side.
 *
 * Blocks while the buffer is full (unless shutting down).
 * Race condition without the mutex: two producers could both read
 * `count < CAPACITY` and both write to the same slot.  The mutex
 * serialises the critical section; `not_full` / `not_empty` condition
 * variables prevent busy-waiting.
 *
 * Returns 0 on success, -1 if shutdown was signalled.
 */
int bounded_buffer_push(bounded_buffer_t *buf, const log_item_t *item)
{
    pthread_mutex_lock(&buf->mutex);
    while (buf->count == LOG_BUFFER_CAPACITY && !buf->shutting_down)
        pthread_cond_wait(&buf->not_full, &buf->mutex);
    if (buf->shutting_down) {
        pthread_mutex_unlock(&buf->mutex);
        return -1;
    }
    buf->items[buf->tail] = *item;
    buf->tail = (buf->tail + 1) % LOG_BUFFER_CAPACITY;
    buf->count++;
    pthread_cond_signal(&buf->not_empty);
    pthread_mutex_unlock(&buf->mutex);
    return 0;
}

/*
 * bounded_buffer_pop — consumer side.
 *
 * Blocks while the buffer is empty.
 * Returns 1 if shutdown and empty (consumer should exit),
 *         0 on successful dequeue.
 */
int bounded_buffer_pop(bounded_buffer_t *buf, log_item_t *item)
{
    pthread_mutex_lock(&buf->mutex);
    while (buf->count == 0 && !buf->shutting_down)
        pthread_cond_wait(&buf->not_empty, &buf->mutex);
    if (buf->count == 0 && buf->shutting_down) {
        pthread_mutex_unlock(&buf->mutex);
        return 1;   /* done */
    }
    *item = buf->items[buf->head];
    buf->head = (buf->head + 1) % LOG_BUFFER_CAPACITY;
    buf->count--;
    pthread_cond_signal(&buf->not_full);
    pthread_mutex_unlock(&buf->mutex);
    return 0;
}

/* ------------------------------------------------------------------ */
/* Consumer (logger) thread                                            */
/* ------------------------------------------------------------------ */

/*
 * logging_thread — drains the bounded buffer and writes to per-container
 * log files.  Continues until the buffer is both shut down and empty,
 * ensuring no log lines are lost on container or supervisor exit.
 */
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;
    int rc;

    while (1) {
        rc = bounded_buffer_pop(&ctx->log_buffer, &item);
        if (rc != 0)
            break;   /* shutdown + empty */

        /* Route to the container's log file */
        container_record_t *rec = NULL;
        pthread_mutex_lock(&ctx->metadata_lock);
        for (container_record_t *c = ctx->containers; c; c = c->next) {
            if (strncmp(c->id, item.container_id, CONTAINER_ID_LEN) == 0) {
                rec = c;
                break;
            }
        }
        if (rec) {
            int fd = open(rec->log_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
            if (fd >= 0) {
                ssize_t written = 0, total = (ssize_t)item.length;
                const char *ptr = item.data;
                while (written < total) {
                    ssize_t n = write(fd, ptr + written, (size_t)(total - written));
                    if (n <= 0) break;
                    written += n;
                }
                close(fd);
            }
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
    }
    return NULL;
}

/* ------------------------------------------------------------------ */
/* Producer thread — reads from a container pipe and pushes to buffer  */
/* ------------------------------------------------------------------ */

static void *producer_thread(void *arg)
{
    producer_arg_t *pa = (producer_arg_t *)arg;
    char raw[LOG_CHUNK_SIZE];
    ssize_t n;
    log_item_t item;

    while ((n = read(pa->pipe_read_fd, raw, sizeof(raw) - 1)) > 0) {
        memset(&item, 0, sizeof(item));
        strncpy(item.container_id, pa->container_id, CONTAINER_ID_LEN - 1);
        item.length = (size_t)n;
        memcpy(item.data, raw, (size_t)n);
        bounded_buffer_push(pa->buffer, &item);
    }

    close(pa->pipe_read_fd);
    free(pa);
    return NULL;
}

/* ------------------------------------------------------------------ */
/* Container child entrypoint                                          */
/* ------------------------------------------------------------------ */

/*
 * child_fn — executed inside the clone()'d child.
 *
 * Sets up:
 *   1. stdout/stderr → supervisor via the log pipe
 *   2. UTS hostname = container id
 *   3. chroot into the container rootfs
 *   4. /proc mount so ps/top work inside
 *   5. nice value
 *   6. exec the requested command
 */
int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    /* Redirect stdout and stderr to the logging pipe */
    dup2(cfg->log_write_fd, STDOUT_FILENO);
    dup2(cfg->log_write_fd, STDERR_FILENO);
    close(cfg->log_write_fd);

    /* UTS namespace: set hostname to container id */
    (void)sethostname(cfg->id, strlen(cfg->id));

    /* Mount namespace: chroot into this container's rootfs */
    if (chdir(cfg->rootfs) != 0) {
        perror("chdir");
        return 1;
    }
    if (chroot(".") != 0) {
        perror("chroot");
        return 1;
    }
    (void)chdir("/");

    /* Mount /proc inside the container */
    mkdir("/proc", 0555);
    if (mount("proc", "/proc", "proc", 0, NULL) != 0) {
        /* non-fatal — /proc may already be mounted */
    }

    /* Apply nice value */
    if (cfg->nice_value != 0)
        (void)nice(cfg->nice_value);

    /* Execute the requested command */
    char *args[] = { "/bin/sh", "-c", cfg->command, NULL };
    execv("/bin/sh", args);

    /* execv failed */
    perror("execv");
    return 1;
}

/* ------------------------------------------------------------------ */
/* Monitor ioctl helpers                                               */
/* ------------------------------------------------------------------ */

int register_with_monitor(int monitor_fd, const char *container_id,
                           pid_t host_pid, unsigned long soft_limit_bytes,
                           unsigned long hard_limit_bytes)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);
    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;
    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);
    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;
    return 0;
}

/* ------------------------------------------------------------------ */
/* Container launch helper                                             */
/* ------------------------------------------------------------------ */

/*
 * launch_container — forks a new namespaced container via clone().
 *
 * Pipe layout:
 *   pipefd[0] = read-end  → stays in supervisor (producer thread reads it)
 *   pipefd[1] = write-end → handed to child (becomes its stdout/stderr)
 *
 * After clone() the write-end is closed in the parent so EOF propagates
 * to the producer thread when the container exits.
 */
static container_record_t *launch_container(supervisor_ctx_t *ctx,
                                             const control_request_t *req)
{
    int pipefd[2];
    if (pipe(pipefd) < 0) {
        perror("pipe");
        return NULL;
    }

    /* Ensure the logs directory exists */
    mkdir(LOG_DIR, 0755);

    /* Allocate and populate the record */
    container_record_t *rec = calloc(1, sizeof(*rec));
    if (!rec) { close(pipefd[0]); close(pipefd[1]); return NULL; }

    strncpy(rec->id, req->container_id, CONTAINER_ID_LEN - 1);
    rec->state            = CONTAINER_STARTING;
    rec->soft_limit_bytes = req->soft_limit_bytes;
    rec->hard_limit_bytes = req->hard_limit_bytes;
    rec->started_at       = time(NULL);
    rec->log_write_fd     = pipefd[1];
    snprintf(rec->log_path, sizeof(rec->log_path), "%s/%s.log", LOG_DIR, rec->id);

    /* Build child config on stack (clone() stack must stay alive until exec) */
    child_config_t *cfg = malloc(sizeof(*cfg));
    if (!cfg) { free(rec); close(pipefd[0]); close(pipefd[1]); return NULL; }
    strncpy(cfg->id,      req->container_id, CONTAINER_ID_LEN - 1);
    strncpy(cfg->rootfs,  req->rootfs,       PATH_MAX - 1);
    strncpy(cfg->command, req->command,      CHILD_COMMAND_LEN - 1);
    cfg->nice_value   = req->nice_value;
    cfg->log_write_fd = pipefd[1];

    /* Allocate a stack for the child */
    char *stack = malloc(STACK_SIZE);
    if (!stack) { free(cfg); free(rec); close(pipefd[0]); close(pipefd[1]); return NULL; }
    char *stack_top = stack + STACK_SIZE;

    int clone_flags = CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD;
    pid_t pid = clone(child_fn, stack_top, clone_flags, cfg);

    /* Free stack — the child has already exec'd or failed */
    free(stack);

    if (pid < 0) {
        perror("clone");
        free(cfg); free(rec);
        close(pipefd[0]); close(pipefd[1]);
        return NULL;
    }

    /* Parent: close write-end so we see EOF when the child exits */
    close(pipefd[1]);
    rec->log_write_fd = -1;

    rec->host_pid = pid;
    rec->state    = CONTAINER_RUNNING;

    /* Register with kernel monitor */
    if (ctx->monitor_fd >= 0)
        register_with_monitor(ctx->monitor_fd, rec->id, pid,
                               rec->soft_limit_bytes, rec->hard_limit_bytes);

    /* Spawn a producer thread for this container's pipe */
    producer_arg_t *pa = malloc(sizeof(*pa));
    if (pa) {
        pa->pipe_read_fd = pipefd[0];
        strncpy(pa->container_id, rec->id, CONTAINER_ID_LEN - 1);
        pa->buffer = &ctx->log_buffer;
        pthread_t pt;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        pthread_create(&pt, &attr, producer_thread, pa);
        pthread_attr_destroy(&attr);
    } else {
        close(pipefd[0]);
    }

    free(cfg);
    return rec;
}

/* ------------------------------------------------------------------ */
/* SIGCHLD handler — reaps children, updates metadata                 */
/* ------------------------------------------------------------------ */

static void sigchld_handler(int sig)
{
    (void)sig;
    int saved_errno = errno;
    int wstatus;
    pid_t pid;

    while ((pid = waitpid(-1, &wstatus, WNOHANG)) > 0) {
        pthread_mutex_lock(&g_ctx.metadata_lock);
        for (container_record_t *c = g_ctx.containers; c; c = c->next) {
            if (c->host_pid != pid)
                continue;

            if (WIFEXITED(wstatus)) {
                c->exit_code  = WEXITSTATUS(wstatus);
                c->exit_signal = 0;
                if (c->stop_requested)
                    c->stop_reason = STOP_REASON_STOPPED;
                else
                    c->stop_reason = STOP_REASON_NORMAL;
                c->state = CONTAINER_EXITED;
            } else if (WIFSIGNALED(wstatus)) {
                c->exit_signal = WTERMSIG(wstatus);
                c->exit_code   = 128 + c->exit_signal;
                if (c->exit_signal == SIGKILL && !c->stop_requested)
                    c->stop_reason = STOP_REASON_HARD_LIMIT;
                else
                    c->stop_reason = STOP_REASON_STOPPED;
                c->state = CONTAINER_KILLED;
            }

            /* Unregister from kernel monitor */
            if (g_ctx.monitor_fd >= 0)
                unregister_from_monitor(g_ctx.monitor_fd, c->id, pid);
            break;
        }
        pthread_mutex_unlock(&g_ctx.metadata_lock);
    }
    errno = saved_errno;
}

static volatile int g_supervisor_shutdown = 0;

static void sigterm_handler(int sig)
{
    (void)sig;
    g_supervisor_shutdown = 1;
}

/* ------------------------------------------------------------------ */
/* Supervisor event loop                                               */
/* ------------------------------------------------------------------ */

static void handle_request(supervisor_ctx_t *ctx, int client_fd,
                            const control_request_t *req)
{
    control_response_t resp;
    memset(&resp, 0, sizeof(resp));

    if (req->kind == CMD_START || req->kind == CMD_RUN) {
        /* Check for duplicate ID */
        pthread_mutex_lock(&ctx->metadata_lock);
        for (container_record_t *c = ctx->containers; c; c = c->next) {
            if (strncmp(c->id, req->container_id, CONTAINER_ID_LEN) == 0 &&
                (c->state == CONTAINER_STARTING || c->state == CONTAINER_RUNNING)) {
                pthread_mutex_unlock(&ctx->metadata_lock);
                resp.status = -1;
                snprintf(resp.message, sizeof(resp.message),
                         "Container '%s' is already running", req->container_id);
                write(client_fd, &resp, sizeof(resp));
                return;
            }
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        container_record_t *rec = launch_container(ctx, req);
        if (!rec) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "Failed to launch container '%s'", req->container_id);
            write(client_fd, &resp, sizeof(resp));
            return;
        }

        pthread_mutex_lock(&ctx->metadata_lock);
        rec->next     = ctx->containers;
        ctx->containers = rec;
        pthread_mutex_unlock(&ctx->metadata_lock);

        resp.status = 0;
        snprintf(resp.message, sizeof(resp.message),
                 "Container '%s' started (pid=%d)", rec->id, rec->host_pid);

        if (req->kind == CMD_RUN) {
            /* Block until the container exits */
            write(client_fd, &resp, sizeof(resp));
            int wstatus = 0;
            waitpid(rec->host_pid, &wstatus, 0);
            /* sigchld_handler may have already updated state; send final status */
            memset(&resp, 0, sizeof(resp));
            resp.status    = WIFEXITED(wstatus) ? WEXITSTATUS(wstatus) : 128 + WTERMSIG(wstatus);
            resp.exit_code = resp.status;
            snprintf(resp.message, sizeof(resp.message),
                     "Container '%s' exited with status %d", rec->id, resp.status);
        }

        write(client_fd, &resp, sizeof(resp));
        return;
    }

    if (req->kind == CMD_PS) {
        char buf[4096];
        int  off = 0;
        off += snprintf(buf + off, sizeof(buf) - off,
                        "%-16s %-8s %-12s %-10s %-12s %-12s %-18s\n",
                        "ID", "PID", "STATE", "EXIT", "SOFT_MiB", "HARD_MiB", "STOP_REASON");
        off += snprintf(buf + off, sizeof(buf) - off,
                        "%-16s %-8s %-12s %-10s %-12s %-12s %-18s\n",
                        "----------------", "--------", "------------",
                        "----------", "------------", "------------", "------------------");
        pthread_mutex_lock(&ctx->metadata_lock);
        for (container_record_t *c = ctx->containers; c; c = c->next) {
            off += snprintf(buf + off, sizeof(buf) - off,
                            "%-16s %-8d %-12s %-10d %-12lu %-12lu %-18s\n",
                            c->id, c->host_pid, state_to_string(c->state),
                            c->exit_code,
                            c->soft_limit_bytes >> 20,
                            c->hard_limit_bytes >> 20,
                            reason_to_string(c->stop_reason));
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp.status = 0;
        snprintf(resp.message, sizeof(resp.message), "%s", buf);
        write(client_fd, &resp, sizeof(resp));
        return;
    }

    if (req->kind == CMD_LOGS) {
        char log_path[PATH_MAX];
        snprintf(log_path, sizeof(log_path), "%s/%s.log", LOG_DIR, req->container_id);
        int fd = open(log_path, O_RDONLY);
        if (fd < 0) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "No logs found for '%s'", req->container_id);
            write(client_fd, &resp, sizeof(resp));
            return;
        }
        resp.status = 0;
        snprintf(resp.message, sizeof(resp.message), "LOG:%s", log_path);
        write(client_fd, &resp, sizeof(resp));
        char chunk[4096];
        ssize_t n;
        while ((n = read(fd, chunk, sizeof(chunk))) > 0)
            write(client_fd, chunk, (size_t)n);
        close(fd);
        return;
    }

    if (req->kind == CMD_STOP) {
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *target = NULL;
        for (container_record_t *c = ctx->containers; c; c = c->next) {
            if (strncmp(c->id, req->container_id, CONTAINER_ID_LEN) == 0) {
                target = c;
                break;
            }
        }
        if (!target || target->state != CONTAINER_RUNNING) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "Container '%s' not running", req->container_id);
            write(client_fd, &resp, sizeof(resp));
            return;
        }
        target->stop_requested = 1;
        target->state = CONTAINER_STOPPED;
        pid_t pid = target->host_pid;
        pthread_mutex_unlock(&ctx->metadata_lock);

        /* Graceful: SIGTERM first, then SIGKILL */
        kill(pid, SIGTERM);
        struct timespec ts = {2, 0};
        nanosleep(&ts, NULL);
        kill(pid, SIGKILL);

        resp.status = 0;
        snprintf(resp.message, sizeof(resp.message),
                 "Container '%s' stopped", req->container_id);
        write(client_fd, &resp, sizeof(resp));
        return;
    }

    resp.status = -1;
    snprintf(resp.message, sizeof(resp.message), "Unknown command");
    write(client_fd, &resp, sizeof(resp));
}

/* ------------------------------------------------------------------ */
/* Supervisor main                                                     */
/* ------------------------------------------------------------------ */

static int run_supervisor(const char *rootfs)
{
    (void)rootfs;
    int rc;

    memset(&g_ctx, 0, sizeof(g_ctx));
    g_ctx.server_fd  = -1;
    g_ctx.monitor_fd = -1;

    /* Initialize synchronisation primitives */
    rc = pthread_mutex_init(&g_ctx.metadata_lock, NULL);
    if (rc) { perror("pthread_mutex_init"); return 1; }

    rc = bounded_buffer_init(&g_ctx.log_buffer);
    if (rc) { perror("bounded_buffer_init"); return 1; }

    /* Open kernel monitor device (optional — continue if not present) */
    g_ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (g_ctx.monitor_fd < 0)
        fprintf(stderr, "[supervisor] /dev/container_monitor not available: %s\n",
                strerror(errno));
    else
        fprintf(stderr, "[supervisor] Kernel monitor opened.\n");

    /* Create control UNIX domain socket */
    unlink(CONTROL_PATH);
    g_ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (g_ctx.server_fd < 0) { perror("socket"); return 1; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(g_ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); return 1;
    }
    if (listen(g_ctx.server_fd, 16) < 0) { perror("listen"); return 1; }

    /* Signal handlers */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigchld_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags   = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);

    sa.sa_handler = sigterm_handler;
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    /* Start logging consumer thread */
    pthread_create(&g_ctx.consumer_thread, NULL, logging_thread, &g_ctx);

    /* Ensure log directory exists */
    mkdir(LOG_DIR, 0755);

    fprintf(stderr, "[supervisor] Started. Listening on %s\n", CONTROL_PATH);

    /* Event loop */
    while (!g_supervisor_shutdown) {
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(g_ctx.server_fd, &rfds);
        struct timeval tv = {1, 0};
        int sel = select(g_ctx.server_fd + 1, &rfds, NULL, NULL, &tv);
        if (sel < 0) {
            if (errno == EINTR) continue;
            perror("select"); break;
        }
        if (sel == 0) continue;

        int client_fd = accept(g_ctx.server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            perror("accept"); continue;
        }

        control_request_t req;
        ssize_t n = read(client_fd, &req, sizeof(req));
        if (n == (ssize_t)sizeof(req))
            handle_request(&g_ctx, client_fd, &req);
        close(client_fd);
    }

    fprintf(stderr, "[supervisor] Shutting down...\n");

    /* Stop all running containers */
    pthread_mutex_lock(&g_ctx.metadata_lock);
    for (container_record_t *c = g_ctx.containers; c; c = c->next) {
        if (c->state == CONTAINER_RUNNING || c->state == CONTAINER_STARTING) {
            c->stop_requested = 1;
            kill(c->host_pid, SIGTERM);
        }
    }
    pthread_mutex_unlock(&g_ctx.metadata_lock);

    /* Reap all children */
    int wstatus;
    while (waitpid(-1, &wstatus, 0) > 0)
        ;

    /* Drain and join the logging consumer */
    bounded_buffer_begin_shutdown(&g_ctx.log_buffer);
    pthread_join(g_ctx.consumer_thread, NULL);
    bounded_buffer_destroy(&g_ctx.log_buffer);

    /* Unregister all containers from the monitor and close device */
    if (g_ctx.monitor_fd >= 0) {
        pthread_mutex_lock(&g_ctx.metadata_lock);
        for (container_record_t *c = g_ctx.containers; c; c = c->next)
            unregister_from_monitor(g_ctx.monitor_fd, c->id, c->host_pid);
        pthread_mutex_unlock(&g_ctx.metadata_lock);
        close(g_ctx.monitor_fd);
    }

    /* Free metadata linked list */
    pthread_mutex_lock(&g_ctx.metadata_lock);
    container_record_t *cur = g_ctx.containers;
    while (cur) {
        container_record_t *nxt = cur->next;
        free(cur);
        cur = nxt;
    }
    g_ctx.containers = NULL;
    pthread_mutex_unlock(&g_ctx.metadata_lock);

    close(g_ctx.server_fd);
    unlink(CONTROL_PATH);
    pthread_mutex_destroy(&g_ctx.metadata_lock);

    fprintf(stderr, "[supervisor] Clean exit.\n");
    return 0;
}

/* ------------------------------------------------------------------ */
/* Client-side control-request path                                   */
/* ------------------------------------------------------------------ */

static int send_control_request(const control_request_t *req)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return 1; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect — is the supervisor running?");
        close(fd);
        return 1;
    }

    if (write(fd, req, sizeof(*req)) != (ssize_t)sizeof(*req)) {
        perror("write");
        close(fd);
        return 1;
    }

    /* Read one response */
    control_response_t resp;
    ssize_t n = read(fd, &resp, sizeof(resp));
    if (n != (ssize_t)sizeof(resp)) {
        fprintf(stderr, "Unexpected response size from supervisor\n");
        close(fd);
        return 1;
    }

    /* For LOGS command, print the log file path hint, then stream remaining data */
    if (req->kind == CMD_LOGS && resp.status == 0) {
        /* resp.message = "LOG:<path>" — print the log content */
        char chunk[4096];
        ssize_t rn;
        while ((rn = read(fd, chunk, sizeof(chunk))) > 0)
            fwrite(chunk, 1, (size_t)rn, stdout);
    } else {
        printf("%s\n", resp.message);
    }

    close(fd);
    return resp.status != 0 ? 1 : 0;
}

/* ------------------------------------------------------------------ */
/* CLI entry points                                                    */
/* ------------------------------------------------------------------ */

static int cmd_start(int argc, char *argv[])
{
    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], CONTAINER_ID_LEN - 1);
    strncpy(req.rootfs,       argv[3], PATH_MAX - 1);
    strncpy(req.command,      argv[4], CHILD_COMMAND_LEN - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], CONTAINER_ID_LEN - 1);
    strncpy(req.rootfs,       argv[3], PATH_MAX - 1);
    strncpy(req.command,      argv[4], CHILD_COMMAND_LEN - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], CONTAINER_ID_LEN - 1);
    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], CONTAINER_ID_LEN - 1);
    return send_control_request(&req);
}

/* ------------------------------------------------------------------ */
/* main                                                                */
/* ------------------------------------------------------------------ */

int main(int argc, char *argv[])
{
    if (argc < 2) { usage(argv[0]); return 1; }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }
    if (strcmp(argv[1], "start") == 0) return cmd_start(argc, argv);
    if (strcmp(argv[1], "run")   == 0) return cmd_run(argc, argv);
    if (strcmp(argv[1], "ps")    == 0) return cmd_ps();
    if (strcmp(argv[1], "logs")  == 0) return cmd_logs(argc, argv);
    if (strcmp(argv[1], "stop")  == 0) return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
