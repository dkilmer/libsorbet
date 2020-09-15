#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
#include "library.h"

typedef struct TestRec {
	int id;
	const char *name;
} test_rec_t;

int main(int argc, const char **argv) {
	sorbet_def sdef;
	sdef.filename = "/home/dmk/data/file.sorbet";
	data_column cols[] = {
			{"id",   INTEGER, NULL_COL_TYPE, NULL_COL_TYPE},
			{"name", STRING,  NULL_COL_TYPE, NULL_COL_TYPE},
	};
	sorbet_schema schema = {sizeof(cols) / sizeof(data_column), cols};
	sdef.schema = schema;
	test_rec_t recs[] = {
			{ 1, "Moe"},
			{2, "Shemp"},
			{3, "Larry"}
	};
	sdef.compression = 1;
	sorbet_writer_open(&sdef);
	for (int i=0; i<3; i++) {
		sorbet_write_int(&sdef, &recs[i].id);
		sorbet_write_string(&sdef, (const uint8_t *)recs[i].name, strlen(recs[i].name));
	}
	sorbet_writer_close(&sdef);
	return 0;
}