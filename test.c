#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include "library.h"

int main(int argc, const char **argv) {
	struct DataColumn cols[] = {
			{"id",   INTEGER, NULL_COL_TYPE, NULL_COL_TYPE},
			{"name", STRING,  NULL_COL_TYPE, NULL_COL_TYPE},
	};
	schema_t schema = {sizeof(cols) / sizeof(data_column_t), cols};
	sorbet_def_t *sdef = sorbet_writer_open("file.sorbet", schema, false, 0, 0, NULL);
/*
	int tsize = 3840;
	uint8_t *bytes = (uint8_t *)malloc(tsize);
	for (int i=0; i<tsize; i++) {
		uint8_t b = (i % 256);
		bytes[i] = b;
	}
	sorbet_write_bytes_raw(sdef, bytes, tsize);
	sorbet_write_bytes_raw(sdef, bytes, tsize);
	free(bytes);
*/
	sorbet_writer_close(sdef);
	printf("size of time_t is %d\n", (int)sizeof(time_t));
	return 0;
}