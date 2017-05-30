#include <zookeeper/zookeeper.h>
#include <naemon/naemon.h>

NEB_API_VERSION(CURRENT_NEB_API_VERSION);
nebmodule *neb_handle;

static zhandle_t *zh;
const char app_name[] = "distman";
const char cluster_name[] = "master";

void watcher (zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx) {
    if (type == ZOO_CREATED_EVENT) {
        zoo_exists(zh, "/robin", 1, NULL);
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: Created event!");
    } else if (type == ZOO_DELETED_EVENT) {
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: Deleted event!");
    } else if (type == ZOO_CHANGED_EVENT) {
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: Changed event!");
    } else if (type == ZOO_CHILD_EVENT) {
        /* Add a new watch so we can detect disconnects etc */
        zoo_get_children(zh, "/", 1, NULL);
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: Child event!");

    } else if (type == ZOO_SESSION_EVENT) {
        if (state == ZOO_CONNECTED_STATE) {
            nm_log(NSLOG_INFO_MESSAGE, "DistMan: Connected to ZooKeeper server!");
        } else if (state == ZOO_CONNECTING_STATE) {
            nm_log(NSLOG_INFO_MESSAGE, "DistMan: Trying to connect to ZooKeeper server...");
        }
    } else if (type == ZOO_NOTWATCHING_EVENT) {
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: Notwatching event!");
    } else {
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: Unknown ZooKeeper event!");
    }

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
}

int handle_host_check (int event_type, void *data) {
    nebstruct_host_check_data *ds = (nebstruct_host_check_data *)data;
    if (ds->type == NEBTYPE_HOSTCHECK_INITIATE) {
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: Overriding host check");
        return NEBERROR_CALLBACKOVERRIDE;
    }
	return NEB_OK;
}

/* Some temporary test config */
/* TODO: Read config from file */
const char app_name[] = "distman";
const char cluster_name[] = "master";

char *rc_to_string(int rc) {
    switch (rc) {
    case ZOK:
        return "operation completed succesfully";
    case ZNODEEXISTS:
        return "the node already exists";
    case ZNONODE:
        return "the parent node does not exist";
    case ZNOAUTH:
        return "the client does not have permission";
    case ZNOCHILDRENFOREPHEMERALS:
        return "cannot create children of ephemeral nodes";
    case ZBADARGUMENTS:
        return "invalid input parameters";
    case ZINVALIDSTATE:
        return "zhandle state is either ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE";
    case ZMARSHALLINGERROR:
        return "failed to marshall a request; possibly, out of memory";
    default:
        return "unknown return code";
    }
}

void watcher (zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx) {
    if (type == ZOO_CREATED_EVENT) {
        zoo_exists(zh, "/robin", 1, NULL);
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: Created event!");
    } else if (type == ZOO_DELETED_EVENT) {
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: Deleted event!");
    } else if (type == ZOO_CHANGED_EVENT) {
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: Changed event!");
    } else if (type == ZOO_CHILD_EVENT) {
        /* Add a new watch so we can detect disconnects etc */
        zoo_get_children(zh, "/", 1, NULL);
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: Child event!");

    } else if (type == ZOO_SESSION_EVENT) {
        if (state == ZOO_CONNECTED_STATE) {
            nm_log(NSLOG_INFO_MESSAGE, "DistMan: Connected to ZooKeeper server!");
        } else if (state == ZOO_CONNECTING_STATE) {
            nm_log(NSLOG_INFO_MESSAGE, "DistMan: Trying to connect to ZooKeeper server...");
        }
    } else if (type == ZOO_NOTWATCHING_EVENT) {
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: Notwatching event!");
    } else {
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: Unknown ZooKeeper event!");
    }

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
}

int handle_host_check (int event_type, void *data) {
    nebstruct_host_check_data *ds = (nebstruct_host_check_data *)data;
    if (ds->type == NEBTYPE_HOSTCHECK_INITIATE) {
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: Overriding host check");
        return NEBERROR_CALLBACKOVERRIDE;
    }
	return NEB_OK;
}

int handle_service_check (int event_type, void *data) {
    nebstruct_service_check_data *ds = (nebstruct_service_check_data *)data;
    if (ds->type == NEBTYPE_SERVICECHECK_INITIATE) {
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: Overriding service check");
        return NEBERROR_CALLBACKOVERRIDE;
    }
    return NEB_OK;
}

int nebmodule_init (int flags, char *arg, nebmodule *handle) {

    char buffer[1024];
    int rc = 0;

    neb_handle = handle;

	event_broker_options = ~0; /* force settings to "everything" */
    neb_register_callback(NEBCALLBACK_SERVICE_CHECK_DATA, neb_handle, 0, handle_service_check);
    neb_register_callback(NEBCALLBACK_HOST_CHECK_DATA, neb_handle, 0, handle_host_check);

    zh = zookeeper_init("127.0.0.1:2181", watcher, 10000, 0, 0, 0);

    /*
     * Set up necessary paths
     * TODO: Make ACL more secure
     */
    sprintf(buffer, "/%s", app_name);
    rc = zoo_create(zh, buffer, "value", 5, &ZOO_OPEN_ACL_UNSAFE, 0, buffer, sizeof(buffer)-1);
    if (rc != ZOK && rc != ZNODEEXISTS) {
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: app node creation failed, reason: %s",
            rc_to_string(rc));
        return -1;
    }
    sprintf(buffer, "/%s/%s", app_name, cluster_name);
    rc = zoo_create(zh, buffer, "value", 5, &ZOO_OPEN_ACL_UNSAFE, 0, buffer, sizeof(buffer)-1);
    if (rc != ZOK && rc != ZNODEEXISTS) {
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: cluster node creation failed, reason: %s",
            rc_to_string(rc));
        return -1;
    }

    /* Create a session node */
    sprintf(buffer, "/%s/%s/guid_", app_name, cluster_name);
    rc = zoo_create(zh, buffer, "value", 5, &ZOO_OPEN_ACL_UNSAFE, (ZOO_SEQUENCE | ZOO_EPHEMERAL), buffer, sizeof(buffer)-1);
    if (rc != ZOK) {
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: session node creation failed, reason: %s",
            rc_to_string(rc));
        return -1;
    }

    // 2. Determine if I am the leader
    // 3. If yes, set leader to true
    // 4. If no, create a watch on the node I created-1

    //zoo_get_children(zh, "/", 1, NULL);
    //struct Stat stat;
    //rc = zoo_get(zh, "/test", 1, buffer, &size, NULL);
    //nm_log(NSLOG_INFO_MESSAGE, "DistMan: RC IS %d", rc);
    zoo_exists(zh, "/robin", 1, NULL);
    return 0;
}

int nebmodule_deinit (int flags, int reason)
{
    zookeeper_close(zh);
	return 0;
}
