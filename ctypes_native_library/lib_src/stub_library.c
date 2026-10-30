/* Synthetic stand-in for a native library with an init / process / free API.
 * No real cryptography or sensitive data is involved. It exists only to show that a compiled
 * C library can be loaded and called through ctypes inside a Connector SDK connector.
 */

static int initialized = 0;

int stub_init(const char *dir) {
    (void)dir;
    initialized = 1;
    return 0;
}

/* Reverses the input buffer in place as a stand-in transform, so callers can verify the
 * round trip without any real token format. Returns 1 if the library is not initialized. */
int stub_transform(char *buffer, int length) {
    if (!initialized) return 1;
    for (int i = 0; i < length / 2; i++) {
        char tmp = buffer[i];
        buffer[i] = buffer[length - 1 - i];
        buffer[length - 1 - i] = tmp;
    }
    return 0;
}

int stub_free(void) {
    initialized = 0;
    return 0;
}
