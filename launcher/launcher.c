/*
 * launcher.c — Bootstrap launcher for the Data Workspace application.
 *
 * This program handles the full setup lifecycle before the Python app runs:
 *   1. Downloads and verifies a portable Python runtime (if not already present)
 *   2. Creates a virtual environment from that runtime
 *   3. Installs pip dependencies (with hash-based change detection to skip
 *      redundant installs)
 *   4. Launches main.py inside the venv
 *
 * Platform support: Windows and Linux/macOS (selected at compile time).
 * External dependency: libcurl (for HTTP downloads with progress/retry).
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <curl/curl.h>

/* ── Platform-specific paths and commands ─────────────────────────────── */

#ifdef _WIN32
#  include <windows.h>
#  define SLEEP_SEC(s)  Sleep((DWORD)((s) * 1000))
#  define PYTHON_RUNTIME ".\\python\\python.exe"
#  define VENV_PYTHON    "venv\\Scripts\\python.exe"
#  define EXTRACT_CMD \
       "powershell -Command \"tar -xf runtime.tar.gz -C .\""
#  define RUNTIME_ARCHIVE "runtime.tar.gz"
#else
#  include <unistd.h>           /* sleep() */
#  define SLEEP_SEC(s)  sleep(s)
#  define PYTHON_RUNTIME  "./python/bin/python3"
#  define VENV_PYTHON     "venv/bin/python"
#  define EXTRACT_CMD \
       "mkdir -p python && tar -xf runtime.tar.gz -C python --strip-components=1"
#  define RUNTIME_ARCHIVE "runtime.tar.gz"
#endif

/* ── Shared constants ─────────────────────────────────────────────────── */

#define REQ_FILE      "requirements.txt"  /* pip requirements to install         */
#define HASH_FILE     ".req_hash"          /* cached djb2 hash of REQ_FILE        */
#define RUNTIME_JSON  "runtime.json"       /* JSON manifest with download URL/SHA */
#define MAX_RETRIES   3                    /* download retry attempts             */
#define MAX_URL_LEN   512
#define MAX_SHA_LEN   65                   /* 64 hex digits + NUL                */

/* ── SHA-256 (RFC 6234 / FIPS 180-4) ─────────────────────────────────── */
/* Minimal self-contained SHA-256 implementation used to verify the        */
/* integrity of downloaded archives against expected hashes in runtime.json */

/* Round constants — first 32 bits of the fractional parts of the cube
   roots of the first 64 primes (2..311). */
static const uint32_t SHA256_K[64] = {
    0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,
    0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
    0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,
    0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
    0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,
    0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
    0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,
    0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
    0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,
    0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
    0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,
    0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
    0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,
    0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
    0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,
    0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2
};

/* Streaming hash context — accumulates input in a 64-byte block buffer
   and maintains the running 256-bit state between calls. */
typedef struct {
    uint8_t  data[64];    /* current partial block            */
    uint32_t datalen;     /* bytes currently in data[]         */
    uint64_t bitlen;      /* total bits processed so far       */
    uint32_t state[8];    /* intermediate hash value (H0..H7)  */
} SHA256_CTX;

/* SHA-256 logical functions (FIPS 180-4 §4.1.2) */
#define ROTR32(x,n) (((x)>>(n))|((x)<<(32-(n))))  /* 32-bit right rotate  */
#define CH(x,y,z)   (((x)&(y))^(~(x)&(z)))         /* Choice              */
#define MAJ(x,y,z)  (((x)&(y))^((x)&(z))^((y)&(z)))/* Majority            */
#define EP0(x)      (ROTR32(x,2)^ROTR32(x,13)^ROTR32(x,22))  /* Σ0      */
#define EP1(x)      (ROTR32(x,6)^ROTR32(x,11)^ROTR32(x,25))  /* Σ1      */
#define SIG0(x)     (ROTR32(x,7)^ROTR32(x,18)^((x)>>3))      /* σ0      */
#define SIG1(x)     (ROTR32(x,17)^ROTR32(x,19)^((x)>>10))    /* σ1      */

/* Process a single 512-bit (64-byte) block through the SHA-256 compression function. */
static void sha256_transform(SHA256_CTX *ctx, const uint8_t *data) {
    uint32_t a,b,c,d,e,f,g,h,t1,t2,m[64];
    uint32_t i,j;

    /* Prepare the message schedule: copy 16 big-endian words, then expand to 64 */
    for (i=0,j=0; i<16; ++i,j+=4)
        m[i] = ((uint32_t)data[j]<<24)|((uint32_t)data[j+1]<<16)
              |((uint32_t)data[j+2]<<8)|((uint32_t)data[j+3]);
    for (; i<64; ++i)
        m[i] = SIG1(m[i-2])+m[i-7]+SIG0(m[i-15])+m[i-16];

    /* Initialize working variables from current hash state */
    a=ctx->state[0]; b=ctx->state[1]; c=ctx->state[2]; d=ctx->state[3];
    e=ctx->state[4]; f=ctx->state[5]; g=ctx->state[6]; h=ctx->state[7];

    /* 64 rounds of compression */
    for (i=0; i<64; ++i) {
        t1=h+EP1(e)+CH(e,f,g)+SHA256_K[i]+m[i];
        t2=EP0(a)+MAJ(a,b,c);
        h=g; g=f; f=e; e=d+t1; d=c; c=b; b=a; a=t1+t2;
    }

    /* Add compressed chunk to running hash */
    ctx->state[0]+=a; ctx->state[1]+=b; ctx->state[2]+=c; ctx->state[3]+=d;
    ctx->state[4]+=e; ctx->state[5]+=f; ctx->state[6]+=g; ctx->state[7]+=h;
}

/* Initialize context with the standard SHA-256 initial hash values (FIPS 180-4 §5.3.3). */
static void sha256_init(SHA256_CTX *ctx) {
    ctx->datalen=0; ctx->bitlen=0;
    ctx->state[0]=0x6a09e667; ctx->state[1]=0xbb67ae85;
    ctx->state[2]=0x3c6ef372; ctx->state[3]=0xa54ff53a;
    ctx->state[4]=0x510e527f; ctx->state[5]=0x9b05688c;
    ctx->state[6]=0x1f83d9ab; ctx->state[7]=0x5be0cd19;
}

/* Feed arbitrary-length data into the hash.  Buffers partial blocks internally
   and transforms whenever a full 64-byte block is accumulated. */
static void sha256_update(SHA256_CTX *ctx, const uint8_t *data, size_t len) {
    size_t i;
    for (i=0; i<len; ++i) {
        ctx->data[ctx->datalen++]=data[i];
        if (ctx->datalen==64) {
            sha256_transform(ctx, ctx->data);
            ctx->bitlen+=512;
            ctx->datalen=0;
        }
    }
}

/* Finalize the hash: apply PKCS padding (0x80 + zeros + 64-bit big-endian
   bit count), run the last transform(s), and write the 32-byte digest. */
static void sha256_final(SHA256_CTX *ctx, uint8_t *hash) {
    uint32_t i=ctx->datalen;

    /* Pad: append a 1-bit, then zeros.  If there isn't room for the 8-byte
       length suffix in this block, we need an extra block. */
    if (ctx->datalen<56) {
        ctx->data[i++]=0x80;
        while (i<56) ctx->data[i++]=0x00;
    } else {
        ctx->data[i++]=0x80;
        while (i<64) ctx->data[i++]=0x00;
        sha256_transform(ctx, ctx->data);
        memset(ctx->data,0,56);
    }

    /* Append total message length in bits as a 64-bit big-endian integer */
    ctx->bitlen += ctx->datalen * 8;
    ctx->data[63]=(uint8_t)(ctx->bitlen);       ctx->data[62]=(uint8_t)(ctx->bitlen>>8);
    ctx->data[61]=(uint8_t)(ctx->bitlen>>16);   ctx->data[60]=(uint8_t)(ctx->bitlen>>24);
    ctx->data[59]=(uint8_t)(ctx->bitlen>>32);   ctx->data[58]=(uint8_t)(ctx->bitlen>>40);
    ctx->data[57]=(uint8_t)(ctx->bitlen>>48);   ctx->data[56]=(uint8_t)(ctx->bitlen>>56);
    sha256_transform(ctx, ctx->data);

    /* Produce the final hash value — serialize the 8 state words as big-endian bytes */
    for (i=0; i<4; ++i) {
        hash[i]    = (ctx->state[0]>>(24-i*8))&0xff;
        hash[i+4]  = (ctx->state[1]>>(24-i*8))&0xff;
        hash[i+8]  = (ctx->state[2]>>(24-i*8))&0xff;
        hash[i+12] = (ctx->state[3]>>(24-i*8))&0xff;
        hash[i+16] = (ctx->state[4]>>(24-i*8))&0xff;
        hash[i+20] = (ctx->state[5]>>(24-i*8))&0xff;
        hash[i+24] = (ctx->state[6]>>(24-i*8))&0xff;
        hash[i+28] = (ctx->state[7]>>(24-i*8))&0xff;
    }
}

/* Compute SHA-256 of a file; writes lowercase hex into hex_out (>= 65 bytes). */
static int sha256_file(const char *path, char *hex_out) {
    FILE *f = fopen(path, "rb");
    if (!f) { fprintf(stderr, "Cannot open %s for hashing\n", path); return -1; }
    SHA256_CTX ctx; sha256_init(&ctx);
    uint8_t buf[8192]; size_t n;
    while ((n = fread(buf, 1, sizeof(buf), f)) > 0)
        sha256_update(&ctx, buf, n);
    fclose(f);
    uint8_t digest[32]; sha256_final(&ctx, digest);
    for (int i=0; i<32; ++i) sprintf(hex_out + i*2, "%02x", digest[i]);
    hex_out[64]='\0';
    return 0;
}

/* ── libcurl callbacks ────────────────────────────────────────────────── */

/* Context passed to write_cb so downloaded bytes go to a specific FILE. */
typedef struct { FILE *fp; } WriteCtx;

/* libcurl write callback — streams received data directly to disk. */
static size_t write_cb(void *ptr, size_t size, size_t nmemb, void *userdata) {
    WriteCtx *wc = (WriteCtx *)userdata;
    return fwrite(ptr, size, nmemb, wc->fp);
}

/* libcurl progress callback — renders a 50-character ASCII progress bar.
   When the server does not provide Content-Length, falls back to showing
   raw byte counts.  Returning non-zero would abort the transfer. */
static int progress_cb(void *clientp,
                        curl_off_t dltotal, curl_off_t dlnow,
                        curl_off_t ultotal, curl_off_t ulnow) {
    (void)clientp; (void)ultotal; (void)ulnow;
    if (dltotal > 0) {
        int pct    = (int)(dlnow * 100 / dltotal);
        int filled = pct / 2;   /* 50-char bar */
        printf("\r  [");
        for (int i=0; i<50; i++) putchar(i < filled ? '#' : '-');
        printf("] %3d%%  (%lld / %lld bytes)  ",
               pct, (long long)dlnow, (long long)dltotal);
    } else {
        printf("\r  Downloading... %lld bytes  ", (long long)dlnow);
    }
    fflush(stdout);
    return 0;
}

/* Download a file from `url` to `dest` on disk.
 *
 * On transient failures (network timeout, server error, stall) the download
 * is retried up to MAX_RETRIES times with linear back-off (attempt * 2 sec).
 * Partial files are removed between attempts so we always start clean.
 *
 * Returns 0 on success, -1 if all attempts fail or on a fatal local error. */
static int download_file(const char *url, const char *dest) {
    int attempt;
    for (attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        if (attempt > 1) {
            int delay = attempt * 2;
            printf("Retry %d/%d in %d seconds...\n", attempt, MAX_RETRIES, delay);
            SLEEP_SEC(delay);
        }

        FILE *fp = fopen(dest, "wb");
        if (!fp) {
            fprintf(stderr, "Cannot open %s for writing\n", dest);
            return -1;
        }

        CURL *curl = curl_easy_init();
        if (!curl) {
            fclose(fp);
            fprintf(stderr, "curl_easy_init() failed\n");
            return -1;
        }

        WriteCtx wc = { fp };
        curl_easy_setopt(curl, CURLOPT_URL,              url);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION,    write_cb);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA,        &wc);
        curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION, progress_cb);
        curl_easy_setopt(curl, CURLOPT_NOPROGRESS,       0L);        /* enable progress_cb   */
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION,   1L);        /* follow HTTP redirects */
        curl_easy_setopt(curl, CURLOPT_MAXREDIRS,        10L);       /* cap redirect depth    */
        curl_easy_setopt(curl, CURLOPT_FAILONERROR,      1L);        /* HTTP 4xx/5xx → error  */
        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT,   30L);       /* 30 s to connect       */
        curl_easy_setopt(curl, CURLOPT_LOW_SPEED_LIMIT,  1024L);     /* abort if < 1 KB/s … */
        curl_easy_setopt(curl, CURLOPT_LOW_SPEED_TIME,   60L);       /* … for 60 s            */

        CURLcode res = curl_easy_perform(curl);
        curl_easy_cleanup(curl);
        fclose(fp);
        printf("\n");

        if (res == CURLE_OK) return 0;

        fprintf(stderr, "Download error (attempt %d/%d): %s\n",
                attempt, MAX_RETRIES, curl_easy_strerror(res));
        remove(dest);  /* delete partial file before retrying */
    }
    return -1;
}

/* ── Minimal runtime.json parser ─────────────────────────────────────── */
/* We avoid pulling in a full JSON library — runtime.json is a small,       */
/* controlled file with a flat structure, so simple string search suffices.  */

/* Look up a JSON string field by key name using naive substring search.
 * Scans for `"key"` then skips whitespace/colon to extract the value.
 *
 * Limitations: does not handle escaped quotes in values, nested objects,
 * or duplicate keys.  Sufficient for our simple flat runtime.json schema.
 *
 * Returns 1 on success, 0 if the key is not found or the value is too long. */
static int json_get_string(const char *json, const char *key,
                            char *out, size_t outlen) {
    char needle[128];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char *p = strstr(json, needle);
    if (!p) return 0;
    p += strlen(needle);
    /* Skip optional whitespace and the colon separator */
    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r' || *p == ':') p++;
    if (*p != '"') return 0;
    p++;  /* skip opening quote */
    size_t i = 0;
    while (*p && *p != '"' && i < outlen - 1) out[i++] = *p++;
    out[i] = '\0';
    return 1;
}

/* Read runtime.json and extract the platform-specific download URL and
 * expected SHA-256 hash.  On Windows we read windows_url / windows_sha256;
 * on Linux/macOS we read linux_url / linux_sha256.  Returns 0 on success. */
static int read_runtime_json(char *url_out, char *sha_out) {
    FILE *f = fopen(RUNTIME_JSON, "r");
    if (!f) { fprintf(stderr, "Cannot open " RUNTIME_JSON "\n"); return -1; }
    char buf[2048];
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    buf[n] = '\0';

#ifdef _WIN32
    const char *url_key = "windows_url", *sha_key = "windows_sha256";
#else
    const char *url_key = "linux_url",   *sha_key = "linux_sha256";
#endif

    if (!json_get_string(buf, url_key, url_out, MAX_URL_LEN) ||
        !json_get_string(buf, sha_key, sha_out, MAX_SHA_LEN)) {
        fprintf(stderr, "Failed to parse required fields from " RUNTIME_JSON "\n");
        return -1;
    }
    return 0;
}

/* ── Helpers shared by all stages ─────────────────────────────────────── */

/* Quick existence check — tries to open the file for reading. */
int file_exists(const char *path) {
    FILE *f = fopen(path, "r");
    if (f) { fclose(f); return 1; }
    return 0;
}

/* Run a shell command, logging it to stdout.  Aborts the launcher on failure
   so that errors in any bootstrap stage are immediately visible. */
void run_cmd(const char *cmd) {
    printf(">> %s\n", cmd);
    int r = system(cmd);
    if (r != 0) {
        fprintf(stderr, "Command failed (exit code %d)\n", r);
        exit(1);
    }
}

/* ── Bootstrap stages ────────────────────────────────────────────────── */
/* Each stage is idempotent: it checks whether its work has already been   */
/* done and returns early if so.  This lets users re-run the launcher      */
/* safely without redundant downloads or installs.                         */

/* Stage 1: Ensure a portable Python interpreter exists locally.
 * If the runtime binary is missing, download the archive specified in
 * runtime.json, verify its SHA-256 integrity, and extract it. */
void ensure_runtime(void) {
    if (file_exists(PYTHON_RUNTIME)) {
        printf("Python runtime present.\n");
        return;
    }

    char url[MAX_URL_LEN], expected_sha[MAX_SHA_LEN];
    if (read_runtime_json(url, expected_sha) != 0) exit(1);

    int need_download = 1;

    /* If the archive already exists, check its integrity before re-downloading */
    if (file_exists(RUNTIME_ARCHIVE) && strcmp(expected_sha, "EXPECTED_SHA256") != 0) {
        printf("Checking existing archive integrity...\n");
        char existing_sha[MAX_SHA_LEN];
        if (sha256_file(RUNTIME_ARCHIVE, existing_sha) == 0 &&
            strcmp(existing_sha, expected_sha) == 0) {
            printf("Existing archive is valid (SHA256 OK), skipping download.\n");
            need_download = 0;
        } else {
            printf("Existing archive is invalid or incomplete, re-downloading...\n");
            remove(RUNTIME_ARCHIVE);
        }
    }

    if (need_download) {
        printf("Downloading Python runtime from:\n  %s\n", url);
        if (download_file(url, RUNTIME_ARCHIVE) != 0) {
            fprintf(stderr, "Failed to download runtime after %d attempts.\n", MAX_RETRIES);
            exit(1);
        }

        /* SHA-256 integrity check */
        printf("Verifying download integrity...\n");
        char actual_sha[MAX_SHA_LEN];
        if (sha256_file(RUNTIME_ARCHIVE, actual_sha) != 0) {
            remove(RUNTIME_ARCHIVE);
            exit(1);
        }
        if (strcmp(expected_sha, "EXPECTED_SHA256") == 0) {
            printf("Warning: SHA256 not configured in " RUNTIME_JSON
                   " — skipping verification.\n");
        } else if (strcmp(actual_sha, expected_sha) != 0) {
            fprintf(stderr,
                    "SHA256 mismatch — aborting.\n"
                    "  expected : %s\n"
                    "  actual   : %s\n",
                    expected_sha, actual_sha);
            remove(RUNTIME_ARCHIVE);
            exit(1);
        } else {
            printf("SHA256 OK: %s\n", actual_sha);
        }
    }

    printf("Extracting runtime...\n");
    run_cmd(EXTRACT_CMD);
}

/* Stage 2: Create a virtual environment from the portable runtime.
 * The venv isolates app dependencies from the base interpreter. */
void ensure_venv(void) {
    if (file_exists(VENV_PYTHON)) {
        printf("Virtual environment exists.\n");
        return;
    }
    printf("Creating virtual environment...\n");
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "%s -m venv venv", PYTHON_RUNTIME);
    run_cmd(cmd);
}

/* Compute a fast djb2 hash of a file's contents.  Used (not for security
 * but) as a cheap change-detection mechanism for requirements.txt so we
 * can skip the expensive `pip install` step when nothing has changed. */
unsigned long hash_file(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) return 0;
    unsigned long hash = 5381;
    int c;
    while ((c = fgetc(f)) != EOF)
        hash = ((hash << 5) + hash) + c;  /* hash * 33 + c */
    fclose(f);
    return hash;
}

/* Compare the current djb2 hash of requirements.txt against the value
 * saved in .req_hash from the last successful install.  Returns true (1)
 * if the file is new or has changed. */
int requirements_changed(void) {
    unsigned long current = hash_file(REQ_FILE);
    FILE *f = fopen(HASH_FILE, "r");
    if (!f) return 1;  /* no stored hash → first run, needs install */
    unsigned long stored;
    fscanf(f, "%lu", &stored);
    fclose(f);
    return stored != current;
}

/* Persist the current djb2 hash so subsequent launches can detect changes. */
void save_requirements_hash(void) {
    unsigned long h = hash_file(REQ_FILE);
    FILE *f = fopen(HASH_FILE, "w");
    if (f) { fprintf(f, "%lu", h); fclose(f); }
}

/* Stage 3: Install pip dependencies if requirements.txt has changed. */
void install_requirements(void) {
    if (!requirements_changed()) {
        printf("Dependencies already satisfied.\n");
        return;
    }

    printf("Installing dependencies...\n");

    char cmd[1024];

    // Upgrade pip first
    snprintf(cmd, sizeof(cmd), "%s -m pip install --upgrade pip", VENV_PYTHON);
    run_cmd(cmd);

    snprintf(cmd, sizeof(cmd),
             "%s -m pip install -r requirements.txt", VENV_PYTHON);
    run_cmd(cmd);

    // Save hash so we don't reinstall next time
    save_requirements_hash();

    printf("Dependencies installed.\n");
}

/* Stage 4: Launch the main Python application using the venv interpreter. */
void run_app(void) {
    printf("Launching application...\n");
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "%s main.py", VENV_PYTHON);
    run_cmd(cmd);
}

/* ── Entry point ─────────────────────────────────────────────────────── */
/* Runs the four bootstrap stages in order, then exits.  Each stage is
 * idempotent, so re-running is always safe. */
int main(void) {
    curl_global_init(CURL_GLOBAL_ALL);
    ensure_runtime();
    ensure_venv();
    install_requirements();
    run_app();
    curl_global_cleanup();
    return 0;
}