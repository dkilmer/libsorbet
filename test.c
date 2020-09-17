#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
#include "sorbet.h"

typedef struct TestRec {
	int id;
	const char *name;
} test_rec_t;

int main(int argc, const char **argv) {
	sorbet_def sdef;
	sdef.filename = "/home/dmk/data/xx/test-cats/peoples.sorbet";
/*
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
*/
	sorbet_reader_open(&sdef);
	int max_str_len = 1;
	int max_bin_len = 1;
	for (int c=0; c<sdef.schema.numCols; c++) {
		printf("%d - %s (%s)\n", c, sdef.schema.cols[c].name, column_type_label[sdef.schema.cols[c].type]);
		if (sdef.schema.cols[c].type == STRING && sdef.cstats[c].cwidth > max_str_len) {
			max_str_len = sdef.cstats[c].cwidth;
		} else if (sdef.schema.cols[c].type == BINARY && sdef.cstats[c].cwidth > max_bin_len) {
			max_bin_len = sdef.cstats[c].cwidth;
		}
	}
	printf("max string length is %d\n", max_str_len);
	int32_t intval;
	int64_t longval;
	float32_t floatval;
	float64_t doubleval;
	bool boolval;
	char *strval[max_str_len+1];
	uint8_t *binval[max_bin_len];
	sorbet_date dateval;
	uint64_t datetimeval;
	sorbet_time timeval;

	for (uint64_t i=0; i<sdef.n_rows; i++) {
		col_val *row = sorbet_read_row(&sdef);
		for (int c=0; c<sdef.schema.numCols; c++) {
			switch (sdef.schema.cols[c].type) {
				case INTEGER: {
					printf(" %d", row[c].intval);
					break;
				}
				case STRING: {
					printf(" %s", row[c].strval);
					break;
				}
				default: {
				}
			}
		}
/*
		for (int c=0; c<sdef.schema.numCols; c++) {
			switch (sdef.schema.cols[c].type) {
				case INTEGER: {
					if (sorbet_read_int(&sdef, &intval)) {
						printf(" %d", intval);
					} else {
						printf(" NULL");
					}
					break;
				}
				case STRING: {
					int strlen = 0;
					if (sorbet_read_string(&sdef, strval, &strlen)) {
						printf(" %s", strval);
					} else {
						printf(" NULL");
					}
					break;
				}
				default: {
				}
			}
		}
*/
		printf("\n");
	}
	sorbet_reader_close(&sdef);
/*
	printf("sizeof col_val is %d\n", (int)sizeof(col_val));
*/
	return 0;
}