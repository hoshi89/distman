#include <naemon/naemon.h>
#include <assert.h>
#include <glib.h>
#include "configuration.h"

void invariant(config_t *cfg) {
    assert(cfg != NULL);
    assert(cfg->zk_host != NULL);
    assert(cfg->zk_app_node != NULL);
    assert(cfg->zk_election_node != NULL);
    assert(cfg->zk_election_prefix != NULL);
    assert(strlen(cfg->zk_host) > 0);
    assert(strlen(cfg->zk_app_node) > 0);
    assert(strlen(cfg->zk_election_node) > 0);
    assert(strlen(cfg->zk_election_prefix) > 0);
}

config_t *init_config(char *cfg_path) {

    GError *error = NULL;
    GKeyFile *config = g_key_file_new();
    config_t *result = malloc(sizeof(result));
    assert(cfg_path != NULL);

    if (g_key_file_load_from_file(config, cfg_path, G_KEY_FILE_NONE, &error)) {
        result->zk_host = strdup(g_key_file_get_string(config, "ZooKeeper", "host", &error));
        result->zk_app_node = strdup(g_key_file_get_string(config, "ZooKeeper", "app_id", &error));
        result->zk_election_node = strdup(g_key_file_get_string(config, "ZooKeeper", "election_node", &error));
        result->zk_election_prefix = strdup(g_key_file_get_string(config, "ZooKeeper", "election_prefix", &error));
    }

    if (error != NULL) {
        g_error_free(error);
    }

    invariant(result);
    return result;
}

void destroy_config(config_t *cfg) {
    assert(cfg != NULL);
    if (cfg->zk_host) free(cfg->zk_host);
    if (cfg->zk_app_node) free(cfg->zk_app_node);
    if (cfg->zk_election_node) free(cfg->zk_election_node);
    if (cfg->zk_election_prefix) free(cfg->zk_election_prefix);
    free(cfg);
}
