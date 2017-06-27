#ifndef INCLUDED_DISTMAN
#define INCLUDED_DISTMAN

#include <naemon/naemon.h>
#include <zookeeper/zookeeper.h>
#include <sys/wait.h>
#include "configuration.h"

int init_distman(config_t *cfg);
int is_leader();
void deinit_distman();

#endif
