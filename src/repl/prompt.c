#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <sys/stat.h>
#include <unistd.h>
#include <signal.h>
#include <setjmp.h>

#ifdef HAVE_IOCTL
#include <sys/ioctl.h>
#endif

#include <glob.h>

#include <lualib.h>
#include <lauxlib.h>

#include "prompt.h"

#if LUA_VERSION_NUM == 501
#define lua_pushglobaltable(L) lua_pushvalue(L, LUA_GLOBALSINDEX)
#define LUA_OK 0
#define lua_rawlen lua_objlen
#endif

#ifdef HAVE_LIBREADLINE
#include <readline/readline.h>
#else

/* This is a simple readline-like function in case readline is not
 * available. */

#define MAXINPUT 1024

static char *readline(char *prompt)
{
    char *line = NULL;
    int k;

    line = malloc(MAXINPUT);
    fflush(stdout);

    return line;
}
