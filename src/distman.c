#include <assert.h>
#include <glib.h>
#include "distman.h"

static zhandle_t *zh;
static char *app_path, *election_path, *session_prefix, *session_path;
static char *previous_node = NULL;
static int leader = 0;

int leader_election()
{
    GList *node_list = NULL, *s, *t;
    char *node = strdup(session_path + strlen(election_path) + 1);
    struct String_vector vector;
    int rc, i;

    nm_log(NSLOG_INFO_MESSAGE, "distman: running leader election");
    rc = zoo_get_children(zh, election_path, 0, &vector);
    if(rc != ZOK) {
        nm_log(NSLOG_RUNTIME_ERROR, "distman: failed to get child nodes from %s", election_path);
        return ERROR;
    }

    /* Get each session node and sort them by sequence number */
    for (i = 0; i < vector.count; i++) {
        node_list = g_list_prepend(node_list, vector.data[i]);
    }
    node_list = g_list_sort(node_list, (GCompareFunc)strcmp);

    /* Determine leader node */
    leader = 1;
    if (strcmp(node, g_list_first(node_list)->data) != 0) {
        leader = 0;
        /* We're not the leader, so keep watch on the session in front of us */
        s = g_list_find_custom(node_list, node, (GCompareFunc)strcmp);
        t = g_list_previous(s);
        if (previous_node) free(previous_node);
        asprintf(&previous_node, "%s/%s", election_path, t->data);
        rc = zoo_exists(zh, previous_node, 1, NULL);
        if (rc != ZOK) {
            nm_log(NSLOG_RUNTIME_ERROR, "distman: failed to get info on node %s", previous_node);
            return ERROR;
        }
        nm_log(NSLOG_INFO_MESSAGE, "distman: leader node is %s", previous_node);
    } else {
        nm_log(NSLOG_INFO_MESSAGE, "distman: local node is leader");
    }
    return OK;
}

void watcher(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx)
{
    if (type == ZOO_CREATED_EVENT) {
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: Created event!");
    } else if (type == ZOO_DELETED_EVENT) {
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: Deleted event (%s)!", path);
        if (strcmp(previous_node, path) == 0) {
            nm_log(NSLOG_INFO_MESSAGE, "DistMan: reelecting leader!");
            leader_election();
        }
    } else if (type == ZOO_CHANGED_EVENT) {
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: Changed event!");
    } else if (type == ZOO_CHILD_EVENT) {
        /* Add a new watch so we can detect disconnects etc */
        zoo_get_children(zh, "/", 1, NULL);
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: Child event!");
    } else if (type == ZOO_SESSION_EVENT) {
        if (state == ZOO_CONNECTED_STATE) {
            /*
            const clientid_t *id = zoo_client_id(zh);
            if (my_id.client_id == 0 || my_id.client_id != id->client_id) {
                nm_log(NSLOG_INFO_MESSAGE, "DistMan: Got a new session id: 0x%llx", id->client_id);
                my_id = *id;
            }
            */
            nm_log(NSLOG_INFO_MESSAGE, "DistMan: Connected to ZooKeeper server!");
        } else if (state == ZOO_CONNECTING_STATE) {
            nm_log(NSLOG_INFO_MESSAGE, "DistMan: Trying to connect to ZooKeeper server...");
        } else if (state == ZOO_EXPIRED_SESSION_STATE) {
            /* TODO: Reconnect */
        }
    } else if (type == ZOO_NOTWATCHING_EVENT) {
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: Notwatching event!");
    } else {
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: Unknown ZooKeeper event!");
    }
    /*
    if (state == ZOO_EXPIRED_SESSION_STATE) {
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: Expired session state");
    } else if (state == ZOO_AUTH_FAILED_STATE) {
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: Auth failed state");
    } else if (state == ZOO_CONNECTING_STATE) {
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: Connecting state");
    } else if (state == ZOO_ASSOCIATING_STATE) {
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: Associating state");
    } else if (state == ZOO_CONNECTED_STATE) {
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: Connected state");
    } else {
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: Unknown ZooKeeper state! %d", state);
    }
    */
}

int is_leader()
{
    return leader;
}

int init_distman(config_t *cfg)
{
    char buf[512];
    int rc = 0;
    assert(cfg != NULL);

    /* Initialize ZooKeeper */
    zh = zookeeper_init(cfg->zk_host, watcher, 10000, 0, 0, 0);
    if (zh == NULL) {
        nm_log(NSLOG_RUNTIME_ERROR, "distman: failed to init ZooKeeper connection");
        return ERROR;
    }

    /* TODO: Fix secure ACL */
    /* TODO: Get our ID and store in file */

    /* Create all necessary nodes */
    asprintf(&app_path, "/%s", cfg->zk_app_node);
    asprintf(&election_path, "/%s/%s", cfg->zk_app_node, cfg->zk_election_node);
    asprintf(&session_prefix, "/%s/%s/%s", cfg->zk_app_node, cfg->zk_election_node, cfg->zk_election_prefix);

    /* App */
    rc = zoo_create(zh, app_path, NULL, 0, &ZOO_OPEN_ACL_UNSAFE, 0, buf, sizeof(buf)-1);
    if (rc != ZOK && rc != ZNODEEXISTS) {
        nm_log(NSLOG_RUNTIME_ERROR, "distman: failed to create app path");
        return ERROR;
    }

    /* Election */
    rc = zoo_create(zh, election_path, NULL, 0, &ZOO_OPEN_ACL_UNSAFE, 0, buf, sizeof(buf)-1);
    if (rc != ZOK && rc != ZNODEEXISTS) {
        nm_log(NSLOG_RUNTIME_ERROR, "distman: failed to create election path");
        return ERROR;
    }

    /* Session */
    rc = zoo_create(zh, session_prefix, NULL, 0, &ZOO_OPEN_ACL_UNSAFE, (ZOO_EPHEMERAL | ZOO_SEQUENCE), buf, sizeof(buf)-1);
    if (rc != ZOK && rc) {
        nm_log(NSLOG_RUNTIME_ERROR, "distman: failed to create session node");
        return ERROR;
    }
    session_path = strdup(buf);

    if (leader_election() == ERROR) {
        nm_log(NSLOG_RUNTIME_ERROR, "distman: failed to elect leader");
        return ERROR;
    }

    /* TODO: subscribe to results */

    /* TODO: subscribe to work */

    return OK;
}

void deinit_distman()
{
    zookeeper_close(zh);
    free(app_path);
    free(election_path);
    free(session_path);
    free(session_prefix);
    if (previous_node) free(previous_node);
}
