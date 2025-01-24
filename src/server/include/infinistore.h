#ifndef LIBINFINISTORE_H
#define LIBINFINISTORE_H

#include <config.h>

int register_server(unsigned long loop_ptr, server_config_t config);
int get_kvmap_len();

#endif  // INFINISTORE_H