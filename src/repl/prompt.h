#ifndef _PROMPT_H_
#define _PROMPT_H_

#include <lualib.h>
#include <lauxlib.h>

#define LUAP_VERSION "0.9"

void luap_setprompts(lua_State *L, const char *single, const char *multi);
void luap_setpromptfuncs(lua_State *L);
void luap_sethistory(lua_State *L, const char *file);
void luap_setname(lua_State *L, const char *name);
void luap_setcolor(lua_State *L, int enable);

void luap_getprompts(lua_State *L, const char **single, const char **multi);
void luap_getpromptfuncs(lua_State *L);
void luap_gethistory(lua_State *L, const char **file);
void luap_getcolor(lua_State *L, int *enabled);
void luap_getname(lua_State *L, const char **name);

void luap_enter(lua_State *L);
char *luap_describe(lua_State *L, int index);
int luap_call(lua_State *L, int n);

#endif