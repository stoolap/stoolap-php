PHP_ARG_WITH([stoolap],
  [for stoolap support],
  [AS_HELP_STRING([--with-stoolap@<:@=DIR@:>@],
    [Include stoolap support. DIR is the directory containing libstoolap])])

if test "$PHP_STOOLAP" != "no"; then

  STOOLAP_LIB_DIR=""

  if test "$PHP_STOOLAP" != "yes"; then
    dnl User specified directory
    if test -r "$PHP_STOOLAP/libstoolap.dylib" -o -r "$PHP_STOOLAP/libstoolap.so"; then
      STOOLAP_LIB_DIR="$PHP_STOOLAP"
    elif test -r "$PHP_STOOLAP/lib/libstoolap.dylib" -o -r "$PHP_STOOLAP/lib/libstoolap.so"; then
      STOOLAP_LIB_DIR="$PHP_STOOLAP/lib"
    fi
  fi

  dnl Auto-detect in common locations
  if test -z "$STOOLAP_LIB_DIR"; then
    for dir in "$ext_srcdir/.." /usr/local/lib /usr/local /usr/lib; do
      if test -r "$dir/libstoolap.dylib" -o -r "$dir/libstoolap.so"; then
        STOOLAP_LIB_DIR="$dir"
        break
      fi
    done
  fi

  if test -z "$STOOLAP_LIB_DIR"; then
    AC_MSG_ERROR([stoolap library not found. Use --with-stoolap=DIR to specify location])
  fi

  dnl Resolve to absolute path
  STOOLAP_LIB_DIR=$(cd "$STOOLAP_LIB_DIR" && pwd)

  PHP_ADD_LIBRARY_WITH_PATH(stoolap, $STOOLAP_LIB_DIR, STOOLAP_SHARED_LIBADD)
  PHP_SUBST(STOOLAP_SHARED_LIBADD)
  STOOLAP_SHARED_LIBADD="$STOOLAP_SHARED_LIBADD -Wl,-rpath,$STOOLAP_LIB_DIR -lpthread"

  PHP_NEW_EXTENSION(stoolap, stoolap.c stoolap_daemon.c, $ext_shared)
  PHP_ADD_MAKEFILE_FRAGMENT
fi
