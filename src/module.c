#include <naemon/naemon.h>
#include "configuration.h"
#include "distman.h"

NEB_API_VERSION(CURRENT_NEB_API_VERSION)

/* Globals */
static nebmodule *neb_handle = NULL;
static config_t *cfg = NULL;

int handle_host_check (int event_type, void *data) {
    nebstruct_host_check_data *ds = (nebstruct_host_check_data *)data;
    if (ds->type == NEBTYPE_HOSTCHECK_INITIATE) {
        if (is_leader()) {
            nm_log(NSLOG_INFO_MESSAGE, "distman: placing host check (%s) on zookeeper job queue", ds->host_name);
        } else {
            nm_log(NSLOG_INFO_MESSAGE, "distman: blocking host check (%s), the leader handles job queue", ds->host_name);
        }
        /*
         * 1. Copy the check result
         * 2. Give work to a worker wproc_run_callback(ds->command_line, ds->timeout, handle_host_check_work, cr, NULL);
         */
        return NEBERROR_CALLBACKOVERRIDE;
    }
	return NEB_OK;
}

int handle_service_check (int event_type, void *data) {
    nebstruct_service_check_data *ds = (nebstruct_service_check_data *)data;
    if (ds->type == NEBTYPE_SERVICECHECK_INITIATE) {
        if (is_leader()) {
            nm_log(NSLOG_INFO_MESSAGE, "distman: placing service check (%s;%s) on zookeeper job queue", ds->service_description, ds->host_name);
        } else {
            nm_log(NSLOG_INFO_MESSAGE, "distman: blocking service check (%s;%s), the leader handles job queue", ds->service_description, ds->host_name);
        }
        /*
         * 1. Copy the check result
         * 2. Give work to a worker wproc_run_callback(ds->command_line, ds->timeout, handle_service_check_work, cr, NULL);
         */
        return NEBERROR_CALLBACKOVERRIDE;
    }
    return NEB_OK;
}

int handle_notification (int event_type, void *data) {
    nebstruct_notification_data *ds = (nebstruct_notification_data *)data;
    if (ds->type == NEBTYPE_NOTIFICATION_START) {
        if (!is_leader()) {
            nm_log(NSLOG_INFO_MESSAGE, "distman: only leader is allowed to notify");
            return NEBERROR_CALLBACKOVERRIDE;
        }
        nm_log(NSLOG_INFO_MESSAGE, "distman: I am leader, allowing notification");
    }
    return NEB_OK;
}

/* Handle external command */

int nebmodule_init (int flags, char *args, nebmodule *handle) {

    neb_set_module_info(handle, NEBMODULE_MODINFO_TITLE, "Naemon Distribution management broker");
    neb_set_module_info(handle, NEBMODULE_MODINFO_AUTHOR, "OP5 AB");
    neb_set_module_info(handle, NEBMODULE_MODINFO_TITLE, "copyright (C) 2017");
    neb_set_module_info(handle, NEBMODULE_MODINFO_VERSION, "1");
    neb_set_module_info(handle, NEBMODULE_MODINFO_LICENSE, "GPL");
    neb_set_module_info(handle, NEBMODULE_MODINFO_DESC, "Distributed monitoring for Naemon using ZooKeeper");

    cfg = init_config(args);
    if (cfg == NULL) {
        nm_log(NSLOG_RUNTIME_ERROR, "distman: failed to parse config file");
        return ERROR;
    }

    if (init_distman(cfg) != OK) {
        nm_log(NSLOG_RUNTIME_ERROR, "distman: failed to initialize");
        return ERROR;
    }

    neb_handle = handle;
    event_broker_options = ~0; /* force settings to "everything" */
    neb_register_callback(NEBCALLBACK_SERVICE_CHECK_DATA, neb_handle, 0, handle_service_check);
    neb_register_callback(NEBCALLBACK_HOST_CHECK_DATA, neb_handle, 0, handle_host_check);
    neb_register_callback(NEBCALLBACK_NOTIFICATION_DATA, neb_handle, 0, handle_notification);
    /* TODO: External commands must be placed on queue */

    return OK;
}

int nebmodule_deinit (int flags, int reason)
{
    deinit_distman();
    destroy_config(cfg);
    return OK;
}
