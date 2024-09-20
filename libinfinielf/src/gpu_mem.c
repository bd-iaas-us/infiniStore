#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netdb.h>
#include <getopt.h>
#include <arpa/inet.h>
#include <time.h>
#include "infelf_gpu_mem.h"

#ifdef HAVE_CUDA
/* "/usr/local/cuda/include/" is added to build include path in the Makefile */
#include "cuda.h"

#define ASSERT(x)   \
    do {            \
        if (!(x)) { \
            fprintf(stdout, "Assertion \"%s\" failed at %s:%d\n", #x, __FILE__, __LINE__);\
        }           \
    } while (0)

#define CUCHECK(stmt)                   \
    do {                                \
        CUresult result = (stmt);       \
        ASSERT(CUDA_SUCCESS == result); \
    } while (0)

/*----------------------------------------------------------------------------*/

static CUcontext cuContext;

/*
 * Debug print information about all available CUDA devices
 */
static void print_gpu_devices_info(void)
{
    int device_count = 0;
    int i;

    CUCHECK(cuDeviceGetCount(&device_count));
    DEBUG("The number of supporting CUDA devices is %d.\n", device_count);

    for (i = 0; i < device_count; i++) {
        CUdevice cu_dev;
        char name[128];
        int pci_bus_id = 0;
        int pci_device_id = 0;
        int pci_func = 0; /*always 0 for CUDA device*/

        CUCHECK(cuDeviceGet(&cu_dev, i));
        CUCHECK(cuDeviceGetName(name, sizeof(name), cu_dev));
        /* PCI bus identifier of the device */
        CUCHECK(cuDeviceGetAttribute(&pci_bus_id, CU_DEVICE_ATTRIBUTE_PCI_BUS_ID, cu_dev));
        /*PCI device (also known as slot) identifier of the device*/
        CUCHECK(cuDeviceGetAttribute(&pci_device_id, CU_DEVICE_ATTRIBUTE_PCI_DEVICE_ID, cu_dev));

        DEBUG("device %d, handle %d, name \"%s\", BDF %02x:%02x.%d\n",
              i, cu_dev, name, pci_bus_id, pci_device_id, pci_func);
    }
}

static int get_gpu_device_id_from_bdf(const char *bdf)
{
    int given_bus_id = 0;
    int given_device_id = 0;
    int given_func = 0;
    int device_count = 0;
    int i;
    int ret_val;

    /* e.g. "3e:02.0" */
    ret_val = sscanf(bdf, "%x:%x.%x", &given_bus_id, &given_device_id, &given_func);
    if (ret_val != 3){
        ERROR("Wrong BDF format \"%s\". Expected format example: \"3e:02.0\", "
              "where 3e - bus id, 02 - device id, 0 - function\n", bdf);
        return -1;
    }
    if (given_func != 0) {
        ERROR("Wrong pci function %d, 0 is expected\n", given_func);
        return -1;
    }
    CUCHECK(cuDeviceGetCount(&device_count));

    if (device_count == 0) {
        ERROR("There are no available devices that support CUDA\n");
        return -1;
    }

    for (i = 0; i < device_count; i++) {
        CUdevice cu_dev;
        int pci_bus_id    = 0;
        int pci_device_id = 0;

        CUCHECK(cuDeviceGet(&cu_dev, i));
        /* PCI bus identifier of the device */
        CUCHECK(cuDeviceGetAttribute(&pci_bus_id, CU_DEVICE_ATTRIBUTE_PCI_BUS_ID, cu_dev));
        /* PCI device (also known as slot) identifier of the device */
        CUCHECK(cuDeviceGetAttribute(&pci_device_id, CU_DEVICE_ATTRIBUTE_PCI_DEVICE_ID, cu_dev));
        if ((pci_bus_id == given_bus_id) && (pci_device_id == given_device_id)){
            return i;
        }
    }
    ERROR("Given BDF \"%s\" doesn't match one of GPU devices\n", bdf);
    /* Return NULL if there are no CUDA capable devices. */
    return -1;
}

int infelf_gpu_init(const char *bdf)
{
    int dev_id;
    CUresult cu_result;

    INFO("initializing CUDA\n");
    cu_result = cuInit(0);
    if (cu_result != CUDA_SUCCESS) {
        ERROR("cuInit(0) returned %d\n", cu_result);
        return -1;
    }

    if (debug) {
        print_gpu_devices_info();
    }
    dev_id = get_gpu_device_id_from_bdf(bdf);
    if (dev_id < 0) {
        ERROR("No CUDA capable device is found\n");
        return -1;
    }

    /* Pick up device by given dev_id - an ordinal in the range [0, cuDeviceGetCount()-1] */
    CUdevice cu_dev;
    CUCHECK(cuDeviceGet(&cu_dev, dev_id));

    DEBUG("creating CUDA Contnext\n");
    /* Create context */
    cu_result = cuCtxCreate(&cuContext, CU_CTX_MAP_HOST, cu_dev);
    if (cu_result != CUDA_SUCCESS) {
        ERROR("cuCtxCreate() error=%d\n", cu_result);
        return -1;
    }

    DEBUG("making it the current CUDA Context\n");
    cu_result = cuCtxSetCurrent(cuContext);
    if (cu_result != CUDA_SUCCESS) {
        ERROR("cuCtxSetCurrent() error=%d\n", cu_result);
        return -1;
    }

    return dev_id;
}

void *infelf_gpu_buffer_alloc(size_t gpu_buf_size, int dev_id)
{
    const size_t gpu_page_size = 64 * 1024;
    size_t aligned_size;
    CUresult cu_result;

    if (dev_id < 0) {
        ERROR("Invalid dev_id");
        return NULL;
    }

    aligned_size = (gpu_buf_size + gpu_page_size - 1) & ~(gpu_page_size - 1);
    DEBUG("cuMemAlloc() of a %zd bytes GPU buffer\n", aligned_size);
    CUdeviceptr d_A;
    cu_result = cuMemAlloc(&d_A, aligned_size);
    if (cu_result != CUDA_SUCCESS) {
        ERROR("cuMemAlloc error=%d\n", cu_result);
        return NULL;
    }
    DEBUG("allocated GPU buffer address at %016llx pointer=%p\n", d_A, (void*)d_A);

    return ((void*)d_A);
}

void infelf_gpu_buffer_free(void *gpu_buff)
{
    CUdeviceptr d_A = (CUdeviceptr) gpu_buff;

    INFO("deallocating RX GPU buffer\n");
    cuMemFree(d_A);
}

void infelf_gpu_close() {
    DEBUG("destroying current CUDA Context\n");
    CUCHECK(cuCtxDestroy(cuContext));
}

#else

int infelf_gpu_init(const char *bdf)
{
    ERROR("Feature is not supported. Define HAVE_CUDA to enable...\n");
    DEBUG("");
    return -1;
}

void *infelf_gpu_buffer_alloc(size_t gpu_buf_size, int dev_id)
{
    ERROR("Feature is not supported. Define HAVE_CUDA to enable...\n");
    return NULL;
}

void infelf_gpu_buffer_free(void *gpu_buff)
{
    ERROR("Feature is not supported. Define HAVE_CUDA to enable...\n");
}

void infelf_gpu_close()
{
    ERROR("Feature is not supported. Define HAVE_CUDA to enable...\n");
}

#endif
