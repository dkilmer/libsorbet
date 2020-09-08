#ifndef LIBSORBET_LIBRARY_H
#define LIBSORBET_LIBRARY_H

#include <stdio.h>
#include <stdbool.h>
#include <stdint.h>
#include <float.h>
#include <endian.h>
#include <time.h>

#define BUF_SIZE 4096

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

typedef enum ColumnType {
	FOREACH_COLTYPE(GENERATE_ENUM_ENUM)
} column_type_t;

static const char *column_type_label[] = {
	FOREACH_COLTYPE(GENERATE_ENUM_STRING)
};

static uint8_t column_type_tag[] = {
	FOREACH_COLTYPE(GENERATE_ENUM_ENUM)
};

static uint8_t column_type_null_tag[] = {
	FOREACH_COLTYPE(GENERATE_ENUM_NULL_TAG)
};

typedef struct SorbetDate {
	int32_t y;
	int32_t m;
	int32_t d;
} sorbet_date_t;

typedef struct SorbetTime {
	int32_t h;
	int32_t m;
	int32_t s;
} sorbet_time_t;

// a struct that defines a single column in the file
typedef struct DataColumn {
	const char *name;
	column_type_t type;
	column_type_t valType;
	column_type_t keyType;
} data_column_t;

typedef struct ColumnStats {
	int32_t cwidth;
	int64_t cnulls;
	int64_t cbads;
	int32_t max_int;
	int64_t max_long;
	float32_t max_float;
	float64_t max_double;
} column_stats_t;

// a struct that defines the file's schema (just an ordered list of columns)
typedef struct Schema {
	int numCols;
	data_column_t *cols;
} schema_t;

typedef struct SorbetDef {
	FILE *f;
	const char *filename;
	schema_t schema;
	int buf_size;
	uint8_t *buf;
	int buf_offset;
	uint64_t n_rows;
	uint64_t uc_size;
	column_stats_t *cstats;
	int32_t cur_col;
} sorbet_def_t;

// open a sorbet writer
sorbet_def_t *sorbet_writer_open(
	const char *filename,
	schema_t schema,
	bool compressed,
	int metadataType,
	int metadataSize,
	const uint8_t* metadata
);
void sorbet_write_int(sorbet_def_t *sdef, const int32_t *v);
void sorbet_write_long(sorbet_def_t *sdef, const int64_t *v);
void sorbet_write_float(sorbet_def_t *sdef, const float32_t *v);
void sorbet_write_double(sorbet_def_t *sdef, const float64_t *v);
void sorbet_write_boolean(sorbet_def_t *sdef, const bool *v);
void sorbet_write_string(sorbet_def_t *sdef, const uint8_t *v, int32_t  len);
void sorbet_write_binary(sorbet_def_t *sdef, const uint8_t *v, int32_t  len);
void sorbet_write_date(sorbet_def_t *sdef, const sorbet_date_t *v);
void sorbet_write_date_time_t(sorbet_def_t *sdef, const time_t *v);
void sorbet_write_datetime(sorbet_def_t *sdef, const int64_t *dt);
void sorbet_write_datetime_time_t(sorbet_def_t *sdef, const time_t *dt);
void sorbet_write_time(sorbet_def_t *sdef, const sorbet_time_t *v);
void sorbet_write_time_time_t(sorbet_def_t *sdef, const time_t *v);
void sorbet_writer_close(sorbet_def_t *sdef);

#endif //LIBSORBET_LIBRARY_H
