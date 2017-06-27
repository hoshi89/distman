#ifndef INCLUDED_CONFIGURATION
#define INCLUDED_CONFIGURATION

typedef struct configuration_struct {
    char *zk_host;
    char *zk_app_node;
    char *zk_election_node;
    char *zk_election_prefix;
} config_t;

config_t *init_config(char *cfg_path);
void destroy_config(config_t *cfg);

#endif
