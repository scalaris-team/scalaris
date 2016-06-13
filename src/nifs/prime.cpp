#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <math.h>

#include <erl_nif.h>

// adapted from http://www.algorithmist.com/index.php/Prime_Sieve_of_Eratosthenes.c

extern "C" {

#define GET(sieve,b) ((sieve[(b)>>5]>>((b)&31))&1)

uint32_t *make_sieve(uint64_t maxn) {
  const size_t P1 = (maxn + 63) / 64; /* = ceil(MAXN/64) */
  const uint64_t P2 = (maxn + 1) / 2; /* = ceil(MAXN/2) */
  const uint32_t P3 = ceil(ceil(sqrt(maxn)) / 2); /* = ceil(ceil(sqrt(MAXN))/2) */
  size_t sieve_size = sizeof(uint32_t)*P1;
  uint32_t *sieve = (uint32_t*) malloc(sieve_size);
  memset(sieve, 0, sieve_size);
  for (uint32_t k = 1; k <= P3; k++) {
    if (GET(sieve,k) == 0) {
      for(uint32_t j = 2*k+1, i = 2*k*(k+1); i < P2; i += j) {
        sieve[i>>5] |= 1<<(i&31);
      }
    }
  }
  return sieve;
}

bool sieve_is_prime(const uint32_t *sieve, int p) {
  return p==2 || (p>2 && (p&1)==1 && (GET(sieve,(p-1)>>1)==0));
}


/**
 * @brief Returns the first prime larger than or equal to N.
 *
 * @param env erlang nif environment
 * @param argc function arity (only one allowed)
 * @param argv function parameters (only one parameter: N)
 *
 * @return a prime larger than the first parameter
 */
static ERL_NIF_TERM get_nearest(ErlNifEnv* env, int argc,
                                const ERL_NIF_TERM argv[]) {
  ErlNifUInt64 i;
  enif_get_uint64(env, argv[0], &i);
  // from https://en.wikipedia.org/wiki/Bertrand%27s_postulate#Better_results
  // Lowell Schoenfeld showed that for n ≥ 2010760, there is always a prime between n and (1 + 1/16597)n.
  const uint64_t size = i >= 2010760 ? i + (i + 16596) / 16597 : 2010760;
  const uint32_t *sieve = make_sieve(size);
  // search for the prime
  for (++i; i <= size; ++i) {
    if (sieve_is_prime(sieve,i)) {
      free(const_cast<uint32_t *>(sieve));
      return enif_make_uint64(env, i);
    }
  }
  free(const_cast<uint32_t *>(sieve));
  // this should not happen ever!
  return enif_make_badarg(env);
}

/**
 * @brief Checks whether the given number N is prime.
 *
 * @param env erlang nif environment
 * @param argc function arity (only one allowed)
 * @param argv function parameters (only one parameter: N)
 *
 * @return "true" or "false" atom
 */
static ERL_NIF_TERM is_prime(ErlNifEnv* env, int argc,
                             const ERL_NIF_TERM argv[]) {
  ErlNifUInt64 i;
  enif_get_uint64(env, argv[0], &i);
  const uint32_t *sieve = make_sieve(i);
  const bool isprime = sieve_is_prime(sieve, i);
  free(const_cast<uint32_t *>(sieve));
  return enif_make_atom(env, isprime ? "true" : "false");
}

/**
 * the list of nifs
 */
static ErlNifFunc nif_funcs[] = {
  {"get_nearest", 1, get_nearest},
  {"is_prime", 1, is_prime}
};

/**
 * upgrade is called when the NIF library is loaded and there is old code of this module with a loaded NIF library
 */
int upgrade(ErlNifEnv* env, void** priv_data, void** old_priv_data, ERL_NIF_TERM load_info) {
  return 0;
}

/**
 * initialise the nif module
 */
ERL_NIF_INIT(prime,nif_funcs,NULL,NULL,upgrade,NULL)

}
