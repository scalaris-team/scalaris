#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <cassert>
#include <cstdlib>
#include <iostream>
#include <string>

#include "erl_nif.h"

#ifdef __MACH__
#include <mach/mach.h>
#include <mach/clock.h>
#include <mach/mach_time.h>
#include <mach/clock_types.h>
#else
#include <time.h>
#endif

extern "C" {

ERL_NIF_TERM timespec2term(ErlNifEnv* env, struct timespec tp) {
return enif_make_tuple(env, 2,
                         enif_make_int64(env, tp.tv_sec),
                         enif_make_int64(env, tp.tv_nsec));
}

#ifdef __MACH__
static ERL_NIF_TERM get_monotonic_clock(ErlNifEnv* env, int argc,
                                        const ERL_NIF_TERM argv[]) {
  clock_serv_t clk_srv;
  kern_return_t res;
  mach_timespec_t time_spec;

  host_get_clock_service(mach_host_self(),
                         SYSTEM_CLOCK,
                         &clk_srv);
  res = clock_get_time(clk_srv, &time_spec);
  if (res != KERN_SUCCESS)
    return enif_make_atom(env, "failed");
  mach_port_deallocate(mach_task_self(), clk_srv);

  return enif_make_tuple(env, 2,
                         enif_make_int64(env, time_spec.tv_sec),
                         enif_make_int64(env, time_spec.tv_nsec));
}

static ERL_NIF_TERM get_monotonic_clock_res(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  clock_serv_t clk_srv;
  kern_return_t res;
  natural_t attr[1];
  mach_msg_type_number_t cnt;

  host_get_clock_service(mach_host_self(),
                         SYSTEM_CLOCK,
                         &clk_srv);

  cnt = sizeof(attr);
  res = clock_get_attributes(clk_srv, CLOCK_GET_TIME_RES, (clock_attr_t) attr, &cnt);
  if (res != KERN_SUCCESS || cnt != 1)
    return enif_make_atom(env, "failed");

  mach_port_deallocate(mach_task_self(), clk_srv);

  return enif_make_tuple(env, 2,
                         enif_make_int64(env, 0),
                         enif_make_int64(env, attr[0]));
}

static ERL_NIF_TERM get_realtime_clock(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  clock_serv_t clk_srv;
  kern_return_t res;
  mach_timespec_t time_spec;

  host_get_clock_service(mach_host_self(),
                         REALTIME_CLOCK,
                         &clk_srv);
  res = clock_get_time(clk_srv, &time_spec);
  if (res != KERN_SUCCESS)
    return enif_make_atom(env, "failed");
  mach_port_deallocate(mach_task_self(), clk_srv);

  return enif_make_tuple(env, 2,
                         enif_make_int64(env, time_spec.tv_sec),
                         enif_make_int64(env, time_spec.tv_nsec));
}

static ERL_NIF_TERM get_realtime_clock_res(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  clock_serv_t clk_srv;
  kern_return_t res;
  natural_t attr[1];
  mach_msg_type_number_t cnt;

  host_get_clock_service(mach_host_self(),
                         REALTIME_CLOCK,
                         &clk_srv);

  cnt = sizeof(attr);
  res = clock_get_attributes(clk_srv, CLOCK_GET_TIME_RES, (clock_attr_t) attr, &cnt);
  if (res != KERN_SUCCESS || cnt != 1)
    return enif_make_atom(env, "failed");

  mach_port_deallocate(mach_task_self(), clk_srv);

  return enif_make_tuple(env, 2,
                         enif_make_int64(env, 0),
                         enif_make_int64(env, attr[0]));
}

static ERL_NIF_TERM get_ptp0_clock(ErlNifEnv* env, int argc,
                                       const ERL_NIF_TERM argv[]) {
  return enif_make_atom(env, "not_supported");
}

static ERL_NIF_TERM get_ptp1_clock(ErlNifEnv* env, int argc,
                                       const ERL_NIF_TERM argv[]) {
  return enif_make_atom(env, "not_supported");
}

static ERL_NIF_TERM get_ptp2_clock(ErlNifEnv* env, int argc,
                                       const ERL_NIF_TERM argv[]) {
  return enif_make_atom(env, "not_supported");
}

static ERL_NIF_TERM get_ptp0_clock_res(ErlNifEnv* env, int argc,
                                       const ERL_NIF_TERM argv[]) {
  return enif_make_atom(env, "not_supported");
}

static ERL_NIF_TERM get_ptp1_clock_res(ErlNifEnv* env, int argc,
                                       const ERL_NIF_TERM argv[]) {
  return enif_make_atom(env, "not_supported");
}

static ERL_NIF_TERM get_ptp2_clock_res(ErlNifEnv* env, int argc,
                                       const ERL_NIF_TERM argv[]) {
  return enif_make_atom(env, "not_supported");
}

#else
static ERL_NIF_TERM get_time(ErlNifEnv* env, clockid_t clk_id) {
  struct timespec tp;

  int res = clock_gettime(clk_id, &tp);
  if(res != 0)
    return enif_make_atom(env, "failed");

  return timespec2term(env, tp);
}

static ERL_NIF_TERM get_ptp_time(ErlNifEnv* env, std::string dev) {
  return enif_make_atom(env, "not_supported");

  struct timespec tp;

  int fd = ::open(dev.c_str(), O_RDONLY);
  if(fd <= 0) {
    perror("open: ");
    return enif_make_atom(env, "failed");
  }

  int res = ::clock_gettime(fd, &tp);
  if(res != 0) {
    perror("clock_gettime: ");
    return enif_make_atom(env, "failed");
  }

  res = ::close(fd);
  if(res != 0) {
    perror("close: ");
    return enif_make_atom(env, "failed");
  }

  return timespec2term(env, tp);
}

static ERL_NIF_TERM get_ptp_resolution(ErlNifEnv* env, std::string dev) {
  return enif_make_atom(env, "not_supported");

  struct timespec tp;

  int fd = open(dev.c_str(), O_RDONLY);
  if(fd < 0)
    return enif_make_atom(env, "failed");

  int res = clock_getres(fd, &tp);
  if(res != 0)
    return enif_make_atom(env, "failed");

  res = close(fd);
  if(res != 0)
    return enif_make_atom(env, "failed");

  return timespec2term(env, tp);
}

static ERL_NIF_TERM get_resolution(ErlNifEnv* env, clockid_t clk_id) {
  struct timespec tp;

  int res = clock_getres(clk_id, &tp);
  if(res != 0)
    return enif_make_atom(env, "failed");

  return timespec2term(env, tp);
}

static ERL_NIF_TERM get_monotonic_clock_res(ErlNifEnv* env, int argc,
                                            const ERL_NIF_TERM argv[]) {
  return get_resolution(env, CLOCK_MONOTONIC);
}


static ERL_NIF_TERM get_monotonic_clock(ErlNifEnv* env, int argc,
                                        const ERL_NIF_TERM argv[]) {
  return get_time(env, CLOCK_MONOTONIC);
}

static ERL_NIF_TERM get_realtime_clock(ErlNifEnv* env, int argc,
                                       const ERL_NIF_TERM argv[]) {
  return get_time(env, CLOCK_REALTIME);
}

static ERL_NIF_TERM get_realtime_clock_res(ErlNifEnv* env, int argc,
                                           const ERL_NIF_TERM argv[]) {
  return get_resolution(env, CLOCK_REALTIME);
}

static ERL_NIF_TERM get_ptp0_clock(ErlNifEnv* env, int argc,
                                       const ERL_NIF_TERM argv[]) {
  return get_ptp_time(env, "/sys/class/ptp/ptp0/dev");
}

static ERL_NIF_TERM get_ptp1_clock(ErlNifEnv* env, int argc,
                                       const ERL_NIF_TERM argv[]) {
  return get_ptp_time(env, "/sys/class/ptp/ptp1/dev");
}

static ERL_NIF_TERM get_ptp2_clock(ErlNifEnv* env, int argc,
                                       const ERL_NIF_TERM argv[]) {
  return get_ptp_time(env, "/sys/class/ptp/ptp2/dev");
}

static ERL_NIF_TERM get_ptp0_clock_res(ErlNifEnv* env, int argc,
                                       const ERL_NIF_TERM argv[]) {
  return get_ptp_resolution(env, "/sys/class/ptp/ptp0/dev");
}

static ERL_NIF_TERM get_ptp1_clock_res(ErlNifEnv* env, int argc,
                                       const ERL_NIF_TERM argv[]) {
  return get_ptp_resolution(env, "/sys/class/ptp/ptp1/dev");
}

static ERL_NIF_TERM get_ptp2_clock_res(ErlNifEnv* env, int argc,
                                       const ERL_NIF_TERM argv[]) {
  return get_ptp_resolution(env, "/sys/class/ptp/ptp2/dev");
}
#endif

/**
 * the list of nifs
 */
static ErlNifFunc nif_funcs[] = {
  {"get_monotonic_clock", 0, get_monotonic_clock},
  {"get_monotonic_clock_res", 0, get_monotonic_clock_res},
  {"get_realtime_clock", 0, get_realtime_clock},
  {"get_realtime_clock_res", 0, get_realtime_clock_res},
  {"get_ptp0_clock", 0, get_ptp0_clock},
  {"get_ptp1_clock", 0, get_ptp1_clock},
  {"get_ptp2_clock", 0, get_ptp2_clock},
  {"get_ptp0_clock_res", 0, get_ptp0_clock_res},
  {"get_ptp1_clock_res", 0, get_ptp1_clock_res},
  {"get_ptp2_clock_res", 0, get_ptp2_clock_res}
};

/**
 * initialise the nif module
 */
ERL_NIF_INIT(clocks,nif_funcs,NULL,NULL,NULL,NULL)

}
