#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <inttypes.h>

void size_str(char *str, size_t ssize, long long size)
{
    long long base, fraction = 0;
    char mag;

    if (size >= (1 << 30)) {
        base = 1 << 30;
        mag = 'g';
    } else if (size >= (1 << 20)) {
        base = 1 << 20;
        mag = 'm';
    } else if (size >= (1 << 10)) {
        base = 1 << 10;
        mag = 'k';
    } else {
        base = 1;
        mag = '\0';
    }

    if (size / base < 10)
        fraction = (size % base) * 10 / base;
    if (fraction) {
        snprintf(str, ssize, "%lld.%lld%c", size / base, fraction, mag);
    } else {
        snprintf(str, ssize, "%lld%c", size / base, mag);
    }
}

void cnt_str(char *str, size_t ssize, long long cnt)
{
    if (cnt >= 1000000000)
        snprintf(str, ssize, "%lldb", cnt / 1000000000);
    else if (cnt >= 1000000)
        snprintf(str, ssize, "%lldm", cnt / 1000000);
    else if (cnt >= 1000)
        snprintf(str, ssize, "%lldk", cnt / 1000);
    else
        snprintf(str, ssize, "%lld", cnt);
}

int size_to_count(int size)
{
    if (size >= (1 << 20))
        return 100;
    else if (size >= (1 << 16))
        return 1000;
    else if (size >= (1 << 10))
        return 10000;
    else
        return 100000;
}

void format_buf(void *buf, int size)
{
    uint8_t *array = buf;
    static uint8_t data;
    int i;

    for (i = 0; i < size; i++)
        array[i] = data++;
}

int verify_buf(void *buf, int size)
{
    static long long total_bytes;
    uint8_t *array = buf;
    static uint8_t data;
    int i;

    for (i = 0; i < size; i++, total_bytes++) {
        if (array[i] != data++) {
            printf("data verification failed byte %lld\n", total_bytes);
            return -1;
        }
    }
    return 0;
}

uint64_t gettime_ns(void)
{
    struct timespec now;

    clock_gettime(CLOCK_MONOTONIC, &now);
    return now.tv_sec * 1000000000 + now.tv_nsec;
}

uint64_t gettime_us(void)
{
    return gettime_ns() / 1000;
}

int sleep_us(unsigned int time_us)
{
    struct timespec spec;

    if (!time_us)
        return 0;

    spec.tv_sec = 0;
    spec.tv_nsec = time_us * 1000;
    return nanosleep(&spec, NULL);
}

