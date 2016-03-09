#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <cassert>
#include <cstdlib>
#include <iostream>
#include <string>

#include "erl_nif.h"

#include <time.h>

extern "C" {

ERL_NIF_TERM timespec2term(ErlNifEnv* env, struct timespec tp) {
return enif_make_tuple(env, 2,
                         enif_make_int64(env, tp.tv_sec),
                         enif_make_int64(env, tp.tv_nsec));
}

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
    std::cout << dev << " " << fd << std::endl;
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
