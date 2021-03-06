#ifndef LIBSORBET_LIBRARY_H
#define LIBSORBET_LIBRARY_H

#include <stdio.h>
#include <stdbool.h>
#include <stdint.h>
#include <float.h>
#include <endian.h>
#include <time.h>
#include <zlib.h>

#define BUF_SIZE 16384
#define Z_WINDOW_BITS 15
#define GZIP_ENCODING 16

// Make sure we have valid sized to typedef to float32_t and float64_t. If these
// aren't 32 bits and 64 bits respectively, you'll need to change the typedefs
// to use types that are (you'll also have to change the definitions of
// __SIZEOF_FLOAT_32__ and __SIZEOF_FLOAT_64__ so the library will compile.
#define __SIZEOF_FLOAT_32__ __SIZEOF_FLOAT__
#define __SIZEOF_FLOAT_64__ __SIZEOF_DOUBLE__

#if __SIZEOF_FLOAT_32__ != 4
#error "float32_t will not be 32 bits. change the __SIZEOF_FLOAT_32__ definition and float32_t typedef"
#endif

#if __SIZEOF_FLOAT_64__ != 8
#error "float64_t will not be 64 bits. change the __SIZEOF_FLOAT_64__ definition and float64_t typedef"
#endif

typedef float float32_t;
typedef double float64_t;

// Make sure the environment is little-endian. Long-term TODO: handle flipping endian-ness
#if __BYTE_ORDER != __LITTLE_ENDIAN
#error "byte order is not little-endian. can't compile on this platform at present"
#endif

// Use macros to generate the column type enum so we can access the type names and
// be sure that the names are always aligned to the enum's actual definition.
#define FOREACH_COLTYPE(COLTYPE) \
        COLTYPE(NULL_COL_TYPE)   \
        COLTYPE(INTEGER)  \
        COLTYPE(LONG)   \
        COLTYPE(FLOAT)  \
        COLTYPE(DOUBLE)  \
        COLTYPE(BOOLEAN)  \
        COLTYPE(STRING)  \
        COLTYPE(BINARY)  \
        COLTYPE(DATE)  \
        COLTYPE(DATETIME)  \
        COLTYPE(TIME)            \
//        COLTYPE(LIST)  \
//        COLTYPE(MAP)  \

#define GENERATE_ENUM_ENUM(ENUM) ENUM,
#define GENERATE_ENUM_STRING(STRING) #STRING,
#define GENERATE_ENUM_NULL_TAG(ENUM) (ENUM + 90),

typedef enum s_column_type {
	FOREACH_COLTYPE(GENERATE_ENUM_ENUM)
} column_type;

static const char *column_type_label[] = {
	FOREACH_COLTYPE(GENERATE_ENUM_STRING)
};

// just the enum as a byte
static uint8_t column_type_tag[] = {
	FOREACH_COLTYPE(GENERATE_ENUM_ENUM)
};

static uint8_t column_type_null_tag[] = {
	FOREACH_COLTYPE(GENERATE_ENUM_NULL_TAG)
};

typedef struct s_sorbet_date {
	uint8_t y;
	uint8_t m;
	uint8_t d;
} sorbet_date;

typedef struct s_sorbet_time {
	uint8_t h;
	uint8_t m;
	uint8_t s;
} sorbet_time;

typedef struct s_bin_val {
	int32_t len;
	uint8_t *val;
} bin_val;

/*
typedef struct s_str_val {
	int32_t len;
	char *val;
} str_val;
*/

typedef union u_col_val {
	int32_t intval;
	int64_t longval;
	float32_t floatval;
	float64_t doubleval;
	bool boolval;
	bin_val strval;
	bin_val binval;
	sorbet_date dateval;
	int64_t datetimeval;
	sorbet_time timeval;
} col_val;

// a struct that defines a single column in the file
typedef struct s_data_column {
	char *name;
	column_type type;
	column_type valType;
	column_type keyType;
} data_column;

typedef struct s_column_stats {
	int32_t cwidth;
	int64_t cnulls;
	int64_t cbads;
	int32_t max_int;
	int64_t max_long;
	float32_t max_float;
	float64_t max_double;
} column_stats;

// a struct that defines the file's schema (just an ordered list of columns)
typedef struct s_sorbet_schema {
	int numCols;
	data_column *cols;
} sorbet_schema;

typedef struct s_sorbet_def {
	const char *filename;
	sorbet_schema schema;
	uint8_t compression;
	int metadataType;
	int metadataSize;
	uint8_t* metadata;
	FILE *f;
	uint8_t buf[BUF_SIZE];
	int buf_size;
	int buf_offset;
	uint64_t n_rows;
	uint64_t uc_size;
	column_stats *cstats;
	int32_t cur_col;
	z_stream zstrm;
	uint8_t zbuf[BUF_SIZE];
	int zflush;
	long read_cnt;
	long row_cnt;
	col_val *row;
} sorbet_def;

int sorbet_version();

// open a sorbet writer
void sorbet_writer_open(sorbet_def *sdef);
void sorbet_writer_close(sorbet_def *sdef);
void sorbet_write_int(sorbet_def *sdef, const int32_t *v);
void sorbet_write_long(sorbet_def *sdef, const int64_t *v);
void sorbet_write_float(sorbet_def *sdef, const float32_t *v);
void sorbet_write_double(sorbet_def *sdef, const float64_t *v);
void sorbet_write_boolean(sorbet_def *sdef, const bool *v);
void sorbet_write_string(sorbet_def *sdef, const uint8_t *v, int32_t len);
void sorbet_write_binary(sorbet_def *sdef, const uint8_t *v, int32_t len);
void sorbet_write_date(sorbet_def *sdef, const sorbet_date *v);
void sorbet_write_date_time_t(sorbet_def *sdef, const time_t *v);
void sorbet_write_datetime(sorbet_def *sdef, const int64_t *dt);
void sorbet_write_datetime_time_t(sorbet_def *sdef, const time_t *dt);
void sorbet_write_time(sorbet_def *sdef, const sorbet_time *v);
void sorbet_write_time_time_t(sorbet_def *sdef, const time_t *v);
void sorbet_write_row(sorbet_def *sdef, col_val *row);

void sorbet_reader_open(sorbet_def *sdef);
bool sorbet_read_int(sorbet_def *sdef, int32_t *v);
bool sorbet_read_long(sorbet_def *sdef, int64_t *v);
bool sorbet_read_float(sorbet_def *sdef, float32_t *v);
bool sorbet_read_double(sorbet_def *sdef, float64_t *v);
bool sorbet_read_boolean(sorbet_def *sdef, bool *v);
bool sorbet_read_string(sorbet_def *sdef, char *v, int32_t *len);
bool sorbet_read_binary(sorbet_def *sdef, uint8_t *v, int32_t *len);
bool sorbet_read_date(sorbet_def *sdef, sorbet_date *v);
bool sorbet_read_datetime(sorbet_def *sdef, int64_t *v);
bool sorbet_read_time(sorbet_def *sdef, sorbet_time *v);
col_val *sorbet_read_row(sorbet_def *sdef);
void sorbet_reader_close(sorbet_def *sdef);
#endif //LIBSORBET_LIBRARY_H
