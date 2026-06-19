lib-targets = libupipe_zvbi

libupipe_zvbi-desc = zvbi interface module
libupipe_zvbi-so-version = 1.0.0
libupipe_zvbi-includes = upipe_zvbienc.h upipe_zvbidec.h
libupipe_zvbi-src = upipe_zvbienc.c upipe_zvbidec.c
libupipe_zvbi-libs = libupipe zvbi-0.2
