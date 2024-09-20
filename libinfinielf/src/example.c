#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <netdb.h>
#include <pthread.h>
#include <rdma/rdma_cma.h>
#include "infelf_rdma.h"
#include "example.h"

/*
 * example:
 *     client sends source rkey/addr/len
 *     server receives source rkey/add/len
 *     server rdma reads "ping" data from source
 *     server sends "go ahead" on rdma read completion
 *     client sends sink rkey/addr/len
 *     server receives sink rkey/addr/len
 *     server rdma writes "pong" data to sink
 *     server sends "go ahead" on rdma write completion
 */

static int get_addr(char *dst, struct sockaddr *addr)
{
    struct addrinfo *res;
    int ret;

    ret = getaddrinfo(dst, NULL, NULL, &res);
    if (ret) {
        ERROR("getaddrinfo failed (%s) - invalid hostname or IP address\n",
              gai_strerror(ret));
        return ret;
    }

    if (res->ai_family == PF_INET)
        memcpy(addr, res->ai_addr, sizeof(struct sockaddr_in));
    else if (res->ai_family == PF_INET6)
        memcpy(addr, res->ai_addr, sizeof(struct sockaddr_in6));
    else
        ret = -1;

    freeaddrinfo(res);
    return ret;
}

static int cma_event_handler(struct rdma_cm_id *cma_id,
                             struct rdma_cm_event *event)
{
    int ret = 0;
    struct infelf_ctrl_t *cb = cma_id->context;
    struct app_ctx_t *app = (struct app_ctx_t *)cb->app_ctx;

    DEBUG("%s: [CMA] cma_event type %s cma_id %p (%s)\n",
          cb->server ? "server" : "client",
          rdma_event_str(event->event), cma_id,
          (cma_id == cb->cm_id) ? "parent" : "child");

    switch (event->event) {
    case RDMA_CM_EVENT_ADDR_RESOLVED:
        ret = rdma_resolve_route(cma_id, 2000);
        if (ret) {
            app->state = ERROR;
            ERROR("[CMA] rdma_resolve_route() failed");
        }
        app->state = ADDR_RESOLVED;
        break;

    case RDMA_CM_EVENT_ROUTE_RESOLVED:
        app->state = ROUTE_RESOLVED;
        break;

    case RDMA_CM_EVENT_CONNECT_REQUEST:
        app->state = CONNECT_REQUEST;
        cb->child_cm_id = cma_id;
        break;

    case RDMA_CM_EVENT_CONNECT_RESPONSE:
        app->state = CONNECTED;
        break;

    case RDMA_CM_EVENT_ESTABLISHED:
        /*
         * Server will wake up when first RECV completes.
         */
        if (!cb->server) {
            app->state = CONNECTED;
        }
        break;

    case RDMA_CM_EVENT_ADDR_ERROR:
    case RDMA_CM_EVENT_ROUTE_ERROR:
    case RDMA_CM_EVENT_CONNECT_ERROR:
    case RDMA_CM_EVENT_UNREACHABLE:
    case RDMA_CM_EVENT_REJECTED:
        ERROR("[CMA] cma event %s, error %d\n",
              rdma_event_str(event->event), event->status);
        ret = -1;
        break;

    case RDMA_CM_EVENT_DISCONNECTED:
        app->state = DISCONNECTED;
        break;

    case RDMA_CM_EVENT_DEVICE_REMOVAL:
        ERROR("[CMA] cma detected device removal!\n");
        app->state = ERROR;
        ret = -1;
        break;

    default:
        ERROR("[CMA] unhandled event: %s, ignoring\n",
              rdma_event_str(event->event));
        break;
    }

    return ret;
}

static int cq_event_handler(struct infelf_ctrl_t *cb)
{
    struct ibv_wc wc;
    struct ibv_recv_wr *bad_wr;
    struct app_ctx_t *app = (struct app_ctx_t *)cb->app_ctx;
    int ret;
    int flushed = 0;

    while ((ret = ibv_poll_cq(cb->cq, 1, &wc)) == 1) {
        ret = 0;

        if (wc.status) {
            if (wc.status == IBV_WC_WR_FLUSH_ERR) {
                flushed = 1;
                continue;

            }
            ERROR("cq completion failed status %d\n", wc.status);
            ret = -1;
            goto error;
        }

        switch (wc.opcode) {
        case IBV_WC_SEND:
            DEBUG("%s: [CQ] send completion\n",
                  cb->server ? "server" : "client");
            break;

        case IBV_WC_RDMA_WRITE:
            DEBUG("%s: [CQ] rdma write completion\n",
                  cb->server ? "server" : "client");
            app->state = RDMA_WRITE_COMPLETE;
            break;

        case IBV_WC_RDMA_READ:
            DEBUG("%s: [CQ] rdma read completion\n",
                  cb->server ? "server" : "client");
            app->state = RDMA_READ_COMPLETE;
            break;

        case IBV_WC_RECV:
            DEBUG("%s: [CQ] recv completion\n",
                  cb->server ? "server" : "client");
            ret = cb->server ? server_recv(cb, &wc) :
                       client_recv(cb, &wc);
            if (ret) {
                ERROR("[CQ] recv wc error: %d\n", ret);
                goto error;
            }

            ret = ibv_post_recv(cb->qp, &cb->rq_wr, &bad_wr);
            if (ret) {
                ERROR("[CQ] post recv error: %d\n", ret);
                goto error;
            }
            break;

        default:
            DEBUG("[CQ] unknown!!!!! completion\n");
            ret = -1;
            goto error;
        }
    }
    if (ret) {
        ERROR("[CQ] poll error %d\n", ret);
        goto error;
    }
    return flushed;

error:
    app->state = ERROR;
    return ret;
}


static void usage(const char *name)
{
    printf("%s -s [-vd] [-S size] [-a addr] [-p port]\n", name);
    printf("%s -c [-vd] [-S size] [-I addr] -a addr [-p port]\n", name);
    printf("\t-c\t\tclient side\n");
    printf("\t-I\t\tsource address to bind to for client.\n");
    printf("\t-s\t\tserver side. To bind to any address with IPv6 use -a ::0\n");
    printf("\t-v\t\tdisplay data to stdout\n");
    printf("\t-d\t\tdebug printfs\n");
    printf("\t-S size \tping data size\n");
    printf("\t-a addr\t\taddress\n");
    printf("\t-p port\t\tport\n");
}

int main(int argc, char *argv[])
{
    struct infelf_ctrl_t *cb;
    struct app_ctx_t *app;
    int op;
    int ret = 0;

    cb = malloc(sizeof(*cb));
    if (!cb)
        return -ENOMEM;

    memset(cb, 0, sizeof(*cb));
    cb->server = -1;
    cb->sin.ss_family = PF_INET;
    cb->port = htobe16(7174);
    cb->cma_event_handler = cma_event_handler;
    cb->cq_event_handler = cq_event_handler;
    cb->app_ctx = (struct app_ctx_t *)calloc(1, sizeof(struct app_ctx_t));
    app = cb->app_ctx;
    app->state = IDLE;
    app->size = 64;

    opterr = 0;
    while ((op = getopt(argc, argv, "a:I:p:S:G:scvd")) != -1) {
        switch (op) {
        case 'a':
            ret = get_addr(optarg, (struct sockaddr *) &cb->sin);
            break;
        case 'I':
            ret = get_addr(optarg, (struct sockaddr *) &cb->ssource);
            break;
        case 'p':
            cb->port = htobe16(atoi(optarg));
            DEBUG("port %d\n", (int) atoi(optarg));
            break;
        case 's':
            cb->server = 1;
            break;
        case 'c':
            cb->server = 0;
            break;
        case 'S':
            app->size = atoi(optarg);
            if ((app->size < INFELF_TEST_MIN_BUFSIZE) ||
                (app->size > (INFELF_BUFSIZE - 1))) {
                ERROR("Invalid size %d "
                       "(valid range is %zd to %d)\n",
                       app->size, INFELF_TEST_MIN_BUFSIZE, INFELF_BUFSIZE);
                ret = EINVAL;
            } else
                DEBUG("size %d\n", (int) atoi(optarg));
            break;
        case 'v':
            app->verbose++;
            break;
        case 'G':
            cb->gpu_bdf = strdup(optarg);
            break;
        case 'd':
            debug++;
            break;
        default:
            usage("example");
            ret = EINVAL;
            goto out;
        }
    }
    if (ret)
        goto out;

    if (cb->server == -1) {
        usage("example");
        ret = EINVAL;
        goto out;
    }

    cb->cm_channel = infelf_create_event_channel();
    if (!cb->cm_channel) {
        ret = errno;
        goto out;
    }

    ret = rdma_create_id(cb->cm_channel, &cb->cm_id, cb, RDMA_PS_TCP);
    if (ret) {
        ERROR("rdma_create_id() failed\n");
        goto out2;
    }
    DEBUG("created cm_id %p\n", cb->cm_id);

    ret = pthread_create(&app->cmthread, NULL, cm_thread, cb);
    if (ret) {
        ERROR("pthread_create() failed");
        goto out2;
    }

    if (cb->server) {
        ret = run_server(cb);
    } else {
        ret = run_client(cb);
    }

    DEBUG("destroy cm_id %p\n", cb->cm_id);
    rdma_destroy_id(cb->cm_id);
out2:
    rdma_destroy_event_channel(cb->cm_channel);
out:
    free(cb);
    return ret;
}
