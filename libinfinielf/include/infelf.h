#ifndef __INFELF_H__
#define __INFELF_H__

static int debug = 1;
#define DEBUG(fmt, ...) \
        do { if (debug) fprintf(stdout, "[DEBUG] " fmt, ##__VA_ARGS__); } while (0)
#define ERROR(fmt, ...) \
        do { if (1) fprintf(stdout, "[ERROR] " fmt, ##__VA_ARGS__); } while (0)
#define INFO(fmt, ...) \
        do { if (1) fprintf(stdout, "[INFO] " fmt, ##__VA_ARGS__); } while (0)

#define _stringify( _x ) # _x
#define stringify( _x ) _stringify( _x )

#endif
