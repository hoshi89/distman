#include <zookeeper/zookeeper.h>
#include <naemon/naemon.h>
#include <sys/wait.h>

NEB_API_VERSION(CURRENT_NEB_API_VERSION)

nebmodule *neb_handle;
int node_flags = 0;

/* Some temporary test config */
/* TODO: Read config from file */
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
        /* TODO: Reconnect */
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

check_result *copy_check_result(check_result *src) {
    check_result *cr = nm_calloc(1, sizeof(*cr));
    init_check_result(cr);
    cr->object_check_type = src->object_check_type;
    cr->host_name = src->host_name ? nm_strdup(src->host_name) : NULL;
    cr->service_description = src->service_description ? nm_strdup(src->service_description) : NULL;
    cr->check_type = src->check_type;
    cr->check_options = src->check_options;
    cr->scheduled_check = src->scheduled_check;
    cr->output_file = src->output_file ? nm_strdup(src->output_file) : NULL;
    cr->output_file_fp = src->output_file_fp;
    cr->latency = src->latency;
    cr->start_time = src->start_time;
    cr->finish_time = src->finish_time;
    cr->early_timeout = src->early_timeout;
    cr->exited_ok = src->exited_ok;
    cr->return_code = src->return_code;
    cr->output = src->output ? nm_strdup(src->output) : NULL;
    return cr;
}

static void handle_host_check_work(wproc_result *wpres, void *arg, int flags) {
	check_result *cr = (check_result *)arg;
	struct host *hst;

	/* decrement the number of host checks still out there... */
	if (currently_running_host_checks > 0)
		currently_running_host_checks--;

	hst = find_host(cr->host_name);
	if (hst && wpres) {
		hst->is_executing = FALSE;
		memcpy(&cr->rusage, &wpres->rusage, sizeof(wpres->rusage));
		cr->start_time.tv_sec = wpres->start.tv_sec;
		cr->start_time.tv_usec = wpres->start.tv_usec;
		cr->finish_time.tv_sec = wpres->stop.tv_sec;
		cr->finish_time.tv_usec = wpres->stop.tv_usec;
		if (WIFEXITED(wpres->wait_status)) {
			cr->return_code = WEXITSTATUS(wpres->wait_status);
		} else {
			cr->return_code = STATE_UNKNOWN;
		}

		if (wpres->outstd && *wpres->outstd) {
			cr->output = nm_strdup(wpres->outstd);
		} else if (wpres->outerr && *wpres->outerr) {
			nm_asprintf(&cr->output, "(No output on stdout) stderr: %s", wpres->outerr);
		} else {
			cr->output = NULL;
		}

		cr->early_timeout = wpres->early_timeout;
		cr->exited_ok = wpres->exited_ok;
		cr->engine = NULL;
		cr->source = wpres->source;

        /*
         * Put check result on the results queue
         */
         nm_log(NSLOG_INFO_MESSAGE, "DistMan: Handling host check %s", cr->host_name);

		process_check_result(cr);
	}
	free_check_result(cr);
	free(cr);
}

int handle_host_check (int event_type, void *data) {
    nebstruct_host_check_data *ds = (nebstruct_host_check_data *)data;
    if (ds->type == NEBTYPE_HOSTCHECK_INITIATE) {
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: Overriding host check");
        check_result *cr = copy_check_result((check_result *)ds->check_result_ptr);
        wproc_run_callback(ds->command_line, ds->timeout, handle_host_check_work, cr, NULL);
        return NEBERROR_CALLBACKOVERRIDE;
    }
	return NEB_OK;
}

static void handle_service_check_work(wproc_result *wpres, void *arg, int flags) {
	check_result *cr = (check_result *)arg;
	if(wpres) {
		memcpy(&cr->rusage, &wpres->rusage, sizeof(wpres->rusage));
		cr->start_time.tv_sec = wpres->start.tv_sec;
		cr->start_time.tv_usec = wpres->start.tv_usec;
		cr->finish_time.tv_sec = wpres->stop.tv_sec;
		cr->finish_time.tv_usec = wpres->stop.tv_usec;
		if (WIFEXITED(wpres->wait_status)) {
			cr->return_code = WEXITSTATUS(wpres->wait_status);
		} else {
			cr->return_code = STATE_UNKNOWN;
		}

		if (wpres->outstd && *wpres->outstd) {
			cr->output = nm_strdup(wpres->outstd);
		} else if (wpres->outerr && *wpres->outerr) {
			nm_asprintf(&cr->output, "(No output on stdout) stderr: %s", wpres->outerr);
		} else {
			cr->output = NULL;
		}

		cr->early_timeout = wpres->early_timeout;
		cr->exited_ok = wpres->exited_ok;
		cr->engine = NULL;
		cr->source = wpres->source;

        /*
         * Put the check result on the results queue
         */
         nm_log(NSLOG_INFO_MESSAGE, "DistMan: Handling service check %s", cr->host_name);

		process_check_result(cr);
	}
	free_check_result(cr);
	free(cr);
}

int handle_service_check (int event_type, void *data) {
    nebstruct_service_check_data *ds = (nebstruct_service_check_data *)data;
    if (ds->type == NEBTYPE_SERVICECHECK_INITIATE) {
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: Overriding service check %s", ds->host_name);
        check_result *cr = copy_check_result((check_result *)ds->check_result_ptr);
        wproc_run_callback(ds->command_line, ds->timeout, handle_service_check_work, cr, NULL);
        return NEBERROR_CALLBACKOVERRIDE;
    }
    return NEB_OK;
}

int handle_notification (int event_type, void *data) {
    nebstruct_notification_data *ds = (nebstruct_notification_data *)data;
    if (ds->type == NEBTYPE_NOTIFICATION_START) {
        /* If I am the leader node, allow notification to proceed */
        /* If I am NOT the leader, override this notification */
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: Overriding notification");
        return NEBERROR_CALLBACKOVERRIDE;
    }
    return NEB_OK;
}

int nebmodule_init (int flags, char *arg, nebmodule *handle) {

    char buffer[1024];
    int rc = 0;
    int i = 0;

    neb_handle = handle;

    event_broker_options = ~0; /* force settings to "everything" */
    neb_register_callback(NEBCALLBACK_SERVICE_CHECK_DATA, neb_handle, 0, handle_service_check);
    neb_register_callback(NEBCALLBACK_HOST_CHECK_DATA, neb_handle, 0, handle_host_check);
    neb_register_callback(NEBCALLBACK_NOTIFICATION_DATA, neb_handle, 0, handle_notification);

    zh = zookeeper_init("127.0.0.1:2181", watcher, 10000, 0, 0, 0);

    /*
     * Set up necessary paths
     * TODO: Make ACL more secure
     * TODO: Leader election
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

    /* Determine the leader node */
    struct Stat stat;
    int buflen = sizeof(buffer);
    rc = zoo_get(zh, buffer, 0, buffer, &buflen, &stat);
    if (rc != ZOK) {
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: failed to get data from session node, reason: %s",
            rc_to_string(rc));
        return -1;
    }

    sprintf(buffer, "/%s/%s", app_name, cluster_name);
    struct String_vector children;
    rc = zoo_get_children(zh, buffer, 0, &children);
    if (rc != ZOK) {
        nm_log(NSLOG_INFO_MESSAGE, "DistMan: failed to get data children, reason: %s",
            rc_to_string(rc));
        return -1;
    }

    for (i = 0; i < children.count; i++) {
        printf("Empheral node owner: %s\n", children.data[i]);
    }

    return 0;
}

int nebmodule_deinit (int flags, int reason)
{
    zookeeper_close(zh);
    return 0;
}
