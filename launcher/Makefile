CC      := gcc
CFLAGS  := -O2 -Wall -Wextra

# ── Platform detection ────────────────────────────────────────────────────────
ifeq ($(OS),Windows_NT)
    TARGET  := launcher.exe
    # Assumes libcurl is available via vcpkg, MSYS2, or a bundled include/lib pair.
    # Adjust CURL_PREFIX if your libcurl lives elsewhere.
    CURL_PREFIX ?= C:/msys64/mingw64
    CFLAGS  += -I$(CURL_PREFIX)/include
    LDFLAGS := -L$(CURL_PREFIX)/lib -lcurl -lws2_32
else
    TARGET  := launcher
    # Uses pkg-config when available; falls back to plain -lcurl.
    CURL_CFLAGS := $(shell pkg-config --cflags libcurl 2>/dev/null)
    CURL_LIBS   := $(shell pkg-config --libs   libcurl 2>/dev/null || echo -lcurl)
    CFLAGS  += $(CURL_CFLAGS)
    LDFLAGS := $(CURL_LIBS)
endif

.PHONY: all clean

all: $(TARGET)

$(TARGET): launcher.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

clean:
	$(RM) $(TARGET) launcher.exe
